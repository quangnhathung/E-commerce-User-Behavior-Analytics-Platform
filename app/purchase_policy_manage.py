#!/usr/bin/env python3
"""Tune and apply versioned purchase thresholds without fitting/building again."""
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
from ecommerce_prediction.contracts import identifier

ARTIFACTS = ROOT / "artifacts/prediction"
POLICIES = ARTIFACTS / "policies"
STATUS = "purchase-policy-status"
ACTIONS = ("tune-purchase-policy", "apply-purchase-policy", STATUS, "test-purchase-policy")


def run(action, values, directory):
    result = launch(action, lambda checksum: {"code_sha256": checksum, **values}, directory,
        profile="purchase-policy", entry="purchase_policy.py", result_prefix="PURCHASE_POLICY_RESULT=",
        spark_job=action != STATUS, monitor=action != STATUS)
    if action != STATUS and (not result.get("passed") or result.get("resources") != {
            "no_oom_or_restart": True, "memory_observations_complete": True}):
        raise infra.InfraError("Purchase policy stage lacks complete result/resource acceptance")
    return result


def selected(explicit, pointer, field):
    return identifier(explicit or json.loads(pointer.read_text())[field])


def dispatch(args):
    if args.action == "tune-purchase-policy":
        model = selected(args.model_version, ARTIFACTS / "latest-purchase-model.json", "model_version")
        result = run(args.action, {"model_version": model}, POLICIES / "attempts")
        key = identifier(result["policy_version"])
        atomic_json(POLICIES / key / "report.json", result)
        atomic_json(POLICIES / "latest-policy.json", {"policy_version": key})
        print(f"Purchase policy evaluated: {key}; eligible={result['eligible']}; threshold={result['threshold']}; reused={result['reused']}")
        if not result["eligible"]:
            print("Policy did not meet both precision/recall targets; application is blocked. Inspect its evaluation report.")
        return
    if args.action in ("apply-purchase-policy", STATUS):
        key = selected(args.policy_version, POLICIES / "latest-policy.json", "policy_version")
        values = {"policy_version": key}
        if args.action == STATUS:
            result = run(args.action, values, POLICIES / key)
            atomic_json(POLICIES / key / "status.json", result)
            return
        # Always select the original pointer. Applying a policy never changes it.
        values["source_score_run_id"] = selected(args.score_run_id, ARTIFACTS / "latest-purchase-score.json", "score_run_id")
        result = run(args.action, values, POLICIES / key)
        score = identifier(result["score_run_id"])
        atomic_json(ARTIFACTS / score / "report.json", result)
        atomic_json(POLICIES / "latest-policy-score.json", {"policy_version": key, "score_run_id": score})
        print(f"Purchase policy scores published: {score}; reused={result['reused']}")
        print(f"Lookup: make purchase-score-status SCORE_RUN_ID={score}")
        return
    result = run(args.action, {}, POLICIES / "tests")
    atomic_json(POLICIES / "test-purchase-policy.json", result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=ACTIONS)
    parser.add_argument("--model-version", default=os.environ.get("MODEL_VERSION") or None)
    parser.add_argument("--policy-version", default=os.environ.get("POLICY_VERSION") or None)
    parser.add_argument("--score-run-id", default=os.environ.get("SCORE_RUN_ID") or None)
    args = parser.parse_args()
    env = infra.read_env(infra.ENV_FILE)
    infra.validate_env(env)
    if args.action != STATUS:
        infra.preflight(env)
    with infra.driver_lock() if args.action != STATUS else nullcontext():
        if args.action != STATUS:
            infra.assert_services()
        dispatch(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Policy command interrupted; committed snapshots are retained for retry.", file=sys.stderr)
        sys.exit(130)
    except (infra.InfraError, OSError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
