#!/usr/bin/env python3
"""Host-only streaming ML CLI; model/seed/config are immutable per run."""
import argparse
from contextlib import nullcontext
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

APP = Path(__file__).resolve().parent
ROOT = APP.parent
sys.path[:0] = [str(APP), str(ROOT)]
from infra import manage as infra
from manage import atomic_json
from streaming_manage import fetch_replay, launch, producer_lock, run_id
from ecommerce_analytics.contracts import sha256_file
from ecommerce_behavior.contracts import BEHAVIOR_VERSION
from ecommerce_ml.contracts import identifier, visitor
from ecommerce_streaming_ml.contracts import DEFAULT_START, DEFAULT_END, PIPELINE_VERSION, validate_request

ARTIFACTS = ROOT / "artifacts/streaming-ml"
ACTIONS = ("prepare-ml-demo", "stream-ml", "replay-ml", "simulate-ml", "stream-ml-status", "test-stream-ml")
SPARK_ACTIONS = {"prepare-ml-demo", "stream-ml", "test-stream-ml"}


def run(action, specification, directory, *, stdin=None, quiet=False):
    spark_job = action in ("prepare", "stream", "test")
    result = launch(action, specification, directory, stdin=stdin, quiet=quiet,
        profile="streaming-ml", entry="streaming_ml.py", result_prefix="STREAM_ML_RESULT=",
        spark_job=spark_job, monitor=spark_job)
    if spark_job and (not result.get("passed") or result.get("resources") != {
            "no_oom_or_restart": True, "memory_observations_complete": True}):
        raise infra.InfraError("Streaming ML lacks complete result/resource acceptance; inspect logs/resources")
    return result


def selected(explicit, relative, field):
    return identifier(explicit or json.loads((ROOT / "artifacts" / relative).read_text())[field])


def prepare_demo(args):
    identity = run_id(args.run_id or "ml-" + uuid.uuid4().hex)
    directory = ARTIFACTS / identity
    previous = json.loads((directory / "config.json").read_text())["request"] if (directory / "config.json").exists() else None
    source = ({"dataset_version": previous["dataset_version"], "silver_path": previous["silver_path"], "quality_status": "passed"}
              if previous else json.loads((ROOT / "artifacts/data/latest-ingest.json").read_text()))
    if source.get("quality_status") != "passed":
        raise ValueError("Streaming ML requires a passed historical ingestion")
    selectors = {
        "segment_model_version": selected(args.segment_model_version or (previous or {}).get("segment_model_version"), "ml/latest-model.json", "model_version"),
        "purchase_model_version": selected(args.purchase_model_version or (previous or {}).get("purchase_model_version"), "prediction/latest-purchase-model.json", "model_version"),
        "policy_version": selected(args.policy_version or (previous or {}).get("policy_version"), "prediction/policies/latest-policy.json", "policy_version")}
    def parameter(name, default):
        explicit = getattr(args, name)
        return explicit if explicit is not None else (previous or {}).get(name, default)
    remote = "/tmp/ecommerce-ml-source-" + uuid.uuid4().hex + ".jsonl"
    def specification(checksum):
        request = {"run_id": identity, "start": parameter("start", DEFAULT_START), "end": parameter("end", DEFAULT_END), "rate": parameter("rate", 10),
            "demo_source": parameter("demo_source", "replay"), "pipeline_version": PIPELINE_VERSION, "behavior_version": BEHAVIOR_VERSION,
            "code_sha256": checksum, "dataset_version": source["dataset_version"], "silver_path": source["silver_path"],
            "base": "/ecommerce", "topic": "ecommerce.events.raw", "dlq_topic": "ecommerce.events.dlq", **selectors}
        validate_request(request)
        if previous is not None and request != previous:
            raise ValueError("RUN_ID config/code/selector changed; restore the pinned version or prepare a new ML run")
        return {"request": request, "local_output": remote}
    result = None
    try:
        result = run("prepare", specification, directory)
        config = result["config"]
        fetch_replay(remote, directory / "events.jsonl", config["replay_sha256"])
        for filename, value in (("config.json", config), ("manifest.json", result["manifest"]), ("prepare.json", result)):
            atomic_json(directory / filename, value)
        atomic_json(ARTIFACTS / "latest-run.json", {"run_id": identity})
    finally:
        if result is not None:
            try:
                infra.execute(infra.compose("exec", "-T", "spark-client", "rm", "-f", remote), capture=True, timeout=20)
            except infra.InfraError:
                print(f"NOTE: Retained owned source file {remote}; inspect this attempt.", file=sys.stderr)
        else:
            print(f"NOTE: If present, retain and inspect owned temporary source {remote} before cleanup.", file=sys.stderr)
    print(f"Prepared streaming ML RUN_ID={identity}; start stream-ml, wait STREAM_ML_READY, then start its producer.")


def dispatch(args):
    if args.action == "prepare-ml-demo":
        return prepare_demo(args)
    if args.action == "test-stream-ml":
        result = run("test", {}, ARTIFACTS / "tests")
        atomic_json(ARTIFACTS / "test-stream-ml.json", result)
        return
    identity = run_id(args.run_id or json.loads((ARTIFACTS / "latest-run.json").read_text())["run_id"])
    directory = ARTIFACTS / identity
    config = json.loads((directory / "config.json").read_text())
    if config["run_id"] != identity or config["pipeline_version"] != PIPELINE_VERSION:
        raise ValueError("Local streaming ML configuration differs from RUN_ID/contract")
    if args.action in ("replay-ml", "simulate-ml"):
        expected = "simulator" if args.action == "simulate-ml" else "replay"
        if config["demo_source"] != expected:
            raise ValueError(f"This run uses {config['demo_source']}; select its matching producer command")
        source = directory / "events.jsonl"
        if sha256_file(source) != config["replay_sha256"]:
            raise ValueError("Streaming ML source checksum changed; retry preparation with the same RUN_ID")
        state = run("status", {"config": config}, directory, quiet=True)
        with source.open("rb") as data:
            for _ in range(state["producer"]["last_seq"] + 1):
                if not data.readline(): raise ValueError("Producer cursor exceeds source count")
            # Canonical replay action reuses the shared producer cancellation guard.
            result = run("replay", {"config": config}, directory, stdin=data)
        filename = "producer.json"
    elif args.action == "stream-ml-status":
        if args.visitor_id is not None: visitor(args.visitor_id)
        result = run("status", {"config": config, "visitor_id": args.visitor_id}, directory)
        filename = "status.json"
    else:
        result = run("stream", {"config": config}, directory)
        filename = "report.json"
    atomic_json(directory / filename, result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=ACTIONS)
    parser.add_argument("--run-id", default=os.environ.get("RUN_ID") or None)
    parser.add_argument("--start", default=os.environ.get("START") or None)
    parser.add_argument("--end", default=os.environ.get("END") or None)
    parser.add_argument("--rate", type=float, default=os.environ.get("RATE") or None)
    parser.add_argument("--demo-source", default=os.environ.get("DEMO_SOURCE") or None)
    parser.add_argument("--segment-model-version", default=os.environ.get("SEGMENT_MODEL_VERSION") or None)
    parser.add_argument("--purchase-model-version", default=os.environ.get("PURCHASE_MODEL_VERSION") or None)
    parser.add_argument("--policy-version", default=os.environ.get("POLICY_VERSION") or None)
    parser.add_argument("--visitor-id", default=os.environ.get("VISITOR_ID") or None)
    args = parser.parse_args()
    env = infra.read_env(infra.ENV_FILE)
    infra.validate_env(env)
    if args.action != "stream-ml-status": infra.preflight(env)
    lock = infra.driver_lock() if args.action in SPARK_ACTIONS else producer_lock() if args.action in ("replay-ml", "simulate-ml") else nullcontext()
    with lock:
        if args.action != "stream-ml-status": infra.assert_services()
        dispatch(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Streaming ML interrupted; retain committed snapshots/checkpoint for retry.", file=sys.stderr)
        sys.exit(130)
    except (infra.InfraError, OSError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
