#!/usr/bin/env python3
"""Host-only CLI for historical features and customer segmentation."""
import argparse
from contextlib import nullcontext
import json
import os
from pathlib import Path
import subprocess
import sys

APP = Path(__file__).resolve().parent
ROOT = APP.parent
sys.path[:0] = [str(APP), str(ROOT)]
from infra import manage as infra
from manage import atomic_json
from streaming_manage import launch
from ecommerce_ml.contracts import DEFAULT_AS_OF, FEATURE_VERSION, identifier, instant, visitor

ARTIFACTS = ROOT / "artifacts/ml"
ACTIONS = ("build-features", "train-segments", "segment-status", "test-ml")


def run(action, specification, directory):
    result = launch(action, specification, directory, profile="ml", entry="ml.py", result_prefix="ML_RESULT=",
                    spark_job=action != "segment-status", monitor=action != "segment-status")
    if action != "segment-status" and (not result.get("passed") or result.get("resources") != {
            "no_oom_or_restart": True, "memory_observations_complete": True}):
        raise infra.InfraError("ML job/result lacks complete resource acceptance; inspect attempts/tests resources.json")
    return result


def selected(pointer, key, explicit):
    return identifier(explicit or json.loads((ARTIFACTS / pointer).read_text())[key])


def dispatch(args):
    if args.action == "build-features":
        instant(args.as_of)
        source = json.loads((ROOT / "artifacts/data/latest-ingest.json").read_text())
        if source.get("quality_status") != "passed":
            raise ValueError("The latest ingestion has not passed its quality checks")
        def spec(code_hash):
            return {"request": {"feature_version": FEATURE_VERSION, "as_of": args.as_of,
                "code_sha256": code_hash, "dataset_version": source["dataset_version"],
                "silver_path": source["silver_path"], "base": "/ecommerce"}}
        result = run(args.action, spec, ARTIFACTS / "attempts")
        key = "feature_snapshot_id"
        pointer = "latest-features.json"
    elif args.action == "train-segments":
        snapshot = selected("latest-features.json", "feature_snapshot_id", args.feature_snapshot_id)
        feature = json.loads((ARTIFACTS / snapshot / "report.json").read_text())
        if not feature.get("passed") or feature["feature_snapshot_id"] != snapshot:
            raise ValueError("Local feature report differs from the selected verified snapshot")
        def spec(code_hash):
            return {"request": {"feature_snapshot_id": snapshot, "feature_path": feature["feature_path"],
                "feature_manifest_sha256": feature["manifest_sha256"], "code_sha256": code_hash, "base": "/ecommerce"}}
        result = run(args.action, spec, ARTIFACTS / "attempts")
        key = "model_version"
        pointer = "latest-model.json"
    elif args.action == "segment-status":
        version = selected("latest-model.json", "model_version", args.model_version)
        if args.visitor_id is not None:
            visitor(args.visitor_id)
        result = run(args.action, {"model_version": version, "visitor_id": args.visitor_id}, ARTIFACTS / version)
        atomic_json(ARTIFACTS / version / "status.json", result)
        return
    else:
        result = run(args.action, {}, ARTIFACTS / "tests")
        atomic_json(ARTIFACTS / "test-ml.json", result)
        return
    # A resource failure must not advance host pointers even if remote publication completed.
    if result.get("resources") != {"no_oom_or_restart": True, "memory_observations_complete": True}:
        raise infra.InfraError("ML job has incomplete resource observations; latest pointer was not advanced")
    identifier(result[key])
    atomic_json(ARTIFACTS / result[key] / "report.json", result)
    atomic_json(ARTIFACTS / pointer, {key: result[key]})
    print(f"{args.action} completed: {result[key]} (reused={result['reused']})")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=ACTIONS)
    parser.add_argument("--as-of", default=os.environ.get("AS_OF") or DEFAULT_AS_OF)
    parser.add_argument("--feature-snapshot-id", default=os.environ.get("FEATURE_SNAPSHOT_ID") or None)
    parser.add_argument("--model-version", default=os.environ.get("MODEL_VERSION") or None)
    parser.add_argument("--visitor-id", default=os.environ.get("VISITOR_ID") or None)
    args = parser.parse_args()
    env = infra.read_env(infra.ENV_FILE)
    infra.validate_env(env)
    if args.action != "segment-status":
        infra.preflight(env)
    with infra.driver_lock() if args.action != "segment-status" else nullcontext():
        if args.action != "segment-status":
            infra.assert_services()
        dispatch(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("ML job interrupted; verified snapshots are retained.", file=sys.stderr)
        sys.exit(130)
    except (infra.InfraError, OSError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
