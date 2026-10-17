#!/usr/bin/env python3
"""Resumable host orchestration; no Python ML dependencies on the host."""
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
from ecommerce_prediction.contracts import CANDIDATES, DEFAULT_SCORE_AS_OF, identifier, instant, visitor

ARTIFACTS = ROOT / "artifacts/prediction"
STATUS_ACTIONS = ("purchase-status", "purchase-score-status")
ACTIONS = ("build-purchase-data", "train-purchase", "purchase-status", "score-purchases", "purchase-score-status", "test-purchase")


def run(action, specification, directory):
    result = launch(action, specification, directory, profile="prediction", entry="prediction.py",
                    result_prefix="PREDICTION_RESULT=", spark_job=action not in STATUS_ACTIONS,
                    monitor=action not in STATUS_ACTIONS)
    if action not in STATUS_ACTIONS and (not result.get("passed") or result.get("resources") != {
            "no_oom_or_restart": True, "memory_observations_complete": True}):
        raise infra.InfraError("Prediction stage lacks complete result/resource acceptance; inspect its log/resources")
    return result


def selected(pointer, key, explicit):
    return identifier(explicit or json.loads((ARTIFACTS / pointer).read_text())[key])


def spec(**values):
    return lambda code_sha256: {"code_sha256": code_sha256, **values}


def save(result, key, pointer):
    identifier(result[key])
    atomic_json(ARTIFACTS / result[key] / "report.json", result)
    atomic_json(ARTIFACTS / pointer, {key: result[key]})


def build_data():
    source = json.loads((ROOT / "artifacts/data/latest-ingest.json").read_text())
    if source.get("quality_status") != "passed":
        raise ValueError("Purchase dataset requires a passed historical ingestion")
    def prepare(code_sha256):
        return {"code_sha256": code_sha256, "request": {"code_sha256": code_sha256,
            "dataset_version": source["dataset_version"], "silver_path": source["silver_path"], "base": "/ecommerce"}}
    prepared = run("prepare-purchase-data", prepare, ARTIFACTS / "attempts")
    config = prepared["config"]
    directory = ARTIFACTS / config["purchase_dataset_id"]
    progress = {"purchase_dataset_id": config["purchase_dataset_id"], "config": config,
                "completed_chunks": 0, "total_chunks": len(prepared["chunks"]), "stages": [], "days": []}
    atomic_json(directory / "progress.json", progress)
    reused = True
    for index, entries in enumerate(prepared["chunks"], 1):
        print(f"PURCHASE_BUILD_CHUNK={index}/{len(prepared['chunks'])}: {entries[0]['as_of']} .. {entries[-1]['as_of']}", flush=True)
        result = run("build-purchase-chunk", spec(config=config, entries=entries), directory)
        reused = reused and result["reused"]
        progress["completed_chunks"] = index
        progress["stages"].append({key: result[key] for key in ("log", "resources", "duration_seconds", "reused")})
        progress["days"].extend(result["days"])
        atomic_json(directory / "progress.json", progress)
    result = run("finalize-purchase-data", spec(config=config), directory)
    result["reused"] = result["reused"] and reused
    result["build_stages"] = progress["stages"]
    save(result, "purchase_dataset_id", "latest-purchase-data.json")
    print(f"Purchase dataset ready: {result['purchase_dataset_id']} (reused={result['reused']})")


def train(args):
    key = selected("latest-purchase-data.json", "purchase_dataset_id", args.purchase_dataset_id)
    data = json.loads((ARTIFACTS / key / "report.json").read_text())
    if not data.get("passed") or data["purchase_dataset_id"] != key:
        raise ValueError("Selected purchase dataset report differs from its ID")
    reference = {name: data[name] for name in ("config", "data_path", "manifest_sha256", "purchase_dataset_id")}
    prepared = run("prepare-purchase-training", spec(data=reference), ARTIFACTS / "attempts")
    training = prepared["training"]
    directory = ARTIFACTS / training["model_version"]
    atomic_json(directory / "training.json", training)
    progress = {"model_version": training["model_version"], "sample_reused": prepared["sample_reused"], "candidates": []}
    atomic_json(directory / "progress.json", progress)
    for parameters in CANDIDATES:
        result = run("train-purchase-candidate", spec(training=training, parameters=parameters), directory)
        progress["candidates"].append(result)
        atomic_json(directory / "progress.json", progress)
    result = run("finish-purchase-training", spec(training=training), directory)
    result["reused"] = result["reused"] and prepared["sample_reused"] and all(row["reused"] for row in progress["candidates"])
    result["candidate_stages"] = [{key: row[key] for key in ("parameters", "log", "resources", "reused", "duration_seconds")}
                                  for row in progress["candidates"]]
    save(result, "model_version", "latest-purchase-model.json")
    print(f"Purchase model published: {result['model_version']} (reused={result['reused']})")


def dispatch(args):
    if args.action == "build-purchase-data":
        return build_data()
    if args.action == "train-purchase":
        return train(args)
    if args.action in ("score-purchases", "purchase-status"):
        key = selected("latest-purchase-model.json", "model_version", args.model_version)
        directory = ARTIFACTS / key
        if args.action == "purchase-status":
            result = run(args.action, spec(model_version=key), directory)
            atomic_json(directory / "status.json", result)
            return
        instant(args.as_of)
        result = run(args.action, spec(request={"model_version": key, "as_of": args.as_of}), directory)
        save(result, "score_run_id", "latest-purchase-score.json")
        print(f"Purchase scores published: {result['score_run_id']} (reused={result['reused']})")
        return
    if args.action == "purchase-score-status":
        key = selected("latest-purchase-score.json", "score_run_id", args.score_run_id)
        if args.visitor_id is not None:
            visitor(args.visitor_id)
        result = run(args.action, spec(score_run_id=key, visitor_id=args.visitor_id), ARTIFACTS / key)
        atomic_json(ARTIFACTS / key / "status.json", result)
        return
    result = run("test-purchase", spec(), ARTIFACTS / "tests")
    atomic_json(ARTIFACTS / "test-purchase.json", result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=ACTIONS)
    parser.add_argument("--purchase-dataset-id", default=os.environ.get("PURCHASE_DATASET_ID") or None)
    parser.add_argument("--model-version", default=os.environ.get("MODEL_VERSION") or None)
    parser.add_argument("--score-run-id", default=os.environ.get("SCORE_RUN_ID") or None)
    parser.add_argument("--visitor-id", default=os.environ.get("VISITOR_ID") or None)
    parser.add_argument("--as-of", default=os.environ.get("AS_OF") or DEFAULT_SCORE_AS_OF)
    args = parser.parse_args()
    env = infra.read_env(infra.ENV_FILE)
    infra.validate_env(env)
    if args.action not in STATUS_ACTIONS:
        infra.preflight(env)
    with infra.driver_lock() if args.action not in STATUS_ACTIONS else nullcontext():
        if args.action not in STATUS_ACTIONS:
            infra.assert_services()
        dispatch(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Prediction command interrupted; committed snapshots are retained for retry.", file=sys.stderr)
        sys.exit(130)
    except (infra.InfraError, OSError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
