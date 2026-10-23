#!/usr/bin/env python3
"""One managed live Spark job; Ctrl+C requests a drain before owned cancellation."""
import argparse
import json
import os
from pathlib import Path
import secrets
import signal
import sys
import threading
import time
import uuid

APP = Path(__file__).resolve().parent
ROOT = APP.parent
sys.path[:0] = [str(APP), str(ROOT)]
from infra import manage as infra
from manage import atomic_json
from streaming_manage import launch, producer_lock
from streaming_ml_manage import selected
from ecommerce_live.contracts import VERSION, live_id, validate
from ecommerce_streaming_ml.contracts import DEFAULT_START, DEFAULT_END, PIPELINE_VERSION
from ecommerce_behavior.contracts import BEHAVIOR_VERSION
from live_monitor import LiveMonitor

ARTIFACTS = ROOT / "artifacts/live"


def invoke(action, spec, directory, *, quiet=False):
    return launch(action, spec, directory, profile="streaming-live", entry="live_demo.py",
                  result_prefix="LIVE_RESULT=", spark_job=action == "live-demo",
                  monitor=action == "live-demo", quiet=quiet,
                  monitor_factory=LiveMonitor,
                  producer_owned=action == "live-demo",
                  wrapper_action="run-live" if action == "live-demo" else "run")


def specification(args, env, directory, owner):
    previous = json.loads((directory / "request.json").read_text()) if (directory / "request.json").exists() else None
    source = previous or json.loads((ROOT / "artifacts/data/latest-ingest.json").read_text())
    if previous is None and source.get("quality_status") != "passed":
        raise ValueError("Live demo requires passed historical ingestion")

    def parameter(key, default):
        value = getattr(args, key)
        return value if value is not None else (previous or {}).get(key, default)

    pinned = {"live_id": args.live_id, "live_version": VERSION, "seed": (previous or {}).get("seed", secrets.token_hex(16)),
        "start": (previous or {}).get("start", DEFAULT_START), "end": (previous or {}).get("end", DEFAULT_END),
        "rate": parameter("rate", 5), "events_per_segment": parameter("events_per_segment", 1000),
        "visitors": parameter("visitors", 200), "items": parameter("items", 100), "max_segments": parameter("max_segments", 0),
        "pipeline_version": PIPELINE_VERSION, "behavior_version": BEHAVIOR_VERSION, "base": "/ecommerce",
        "dataset_version": source["dataset_version"], "silver_path": source["silver_path"],
        "topic": "ecommerce.events.raw", "dlq_topic": "ecommerce.events.dlq",
        "minimum_disk_gib": (previous or {}).get("minimum_disk_gib", float(env["MIN_DOCKER_DISK_GIB"]))}
    for key, path, field in [("segment_model_version", "ml/latest-model.json", "model_version"),
            ("purchase_model_version", "prediction/latest-purchase-model.json", "model_version"),
            ("policy_version", "prediction/policies/latest-policy.json", "policy_version")]:
        pinned[key] = selected((previous or {}).get(key), path, field)

    def build(checksum):
        request = {**pinned, "code_sha256": checksum}
        validate(request)
        if previous is not None and request != previous:
            raise ValueError("LIVE_ID config/code changed; restore it or choose a new LIVE_ID")
        atomic_json(directory / "request.json", request)
        atomic_json(directory / "control.json", {"live_id": args.live_id, "owner": owner})
        return {"request": request, "owner": owner}
    return build


def run_demo(args, env, directory):
    owner = uuid.uuid4().hex
    spec = specification(args, env, directory, owner)
    requested, done = threading.Event(), threading.Event()
    interruptions = 0

    def interrupt(signum, frame):
        nonlocal interruptions
        interruptions += 1
        if interruptions > 1:
            raise KeyboardInterrupt
        requested.set()
        try:
            print("\nStopping live input and draining ACKs; keep this terminal open (up to 180s).", flush=True)
        except OSError:
            pass  # The terminal may already be closing after SIGHUP.

    def stop_when_requested():
        while not done.wait(.2):
            if requested.is_set():
                deadline = time.monotonic() + 180
                while not done.is_set() and time.monotonic() < deadline:
                    try:
                        invoke("stop-live", {"live_id": args.live_id, "owner": owner}, directory, quiet=True)
                        if not done.wait(210):
                            print("Live drain did not finish; requesting owned process cancellation.", flush=True)
                            os.kill(os.getpid(), signal.SIGINT)
                        return
                    except (infra.InfraError, OSError):
                        done.wait(2)
                if not done.is_set():
                    os.kill(os.getpid(), signal.SIGINT)
                return

    signals = {signal.SIGINT, signal.SIGTERM}
    if hasattr(signal, "SIGHUP"):
        signals.add(signal.SIGHUP)
    previous = {sig: signal.signal(sig, interrupt) for sig in sorted(signals)}
    stopper = threading.Thread(target=stop_when_requested, daemon=True)
    stopper.start()
    result = None
    try:
        atomic_json(directory / "report.json", {"passed": False, "status": "starting", "live_id": args.live_id})
        result = invoke("live-demo", spec, directory)
        accepted = result.get("passed") is True and result.get("resources") == {
            "no_oom_or_restart": True, "memory_observations_complete": True}
        atomic_json(directory / "report.json", {**result, "passed": accepted})
        if not accepted:
            raise infra.InfraError("Live resource/result acceptance failed; inspect artifacts/live logs/resources")
        print("Live session " + result["status"] + "; snapshots and cursor retained.")
    except BaseException as error:
        atomic_json(directory / "report.json", {**(result or {}), "passed": False, "live_id": args.live_id,
            "status": "interrupted" if isinstance(error, KeyboardInterrupt) else "failed", "error_type": type(error).__name__})
        raise
    finally:
        done.set(); stopper.join(timeout=40)
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("live-demo", "live-status", "live-stop"))
    parser.add_argument("--live-id", default=os.environ.get("LIVE_ID"))
    for flag, variable, kind in [("rate", "RATE", float), ("events-per-segment", "LIVE_EVENTS", int),
            ("visitors", "LIVE_VISITORS", int), ("items", "LIVE_ITEMS", int), ("max-segments", "LIVE_SEGMENTS", int)]:
        parser.add_argument("--" + flag, type=kind, default=os.environ.get(variable) or None)
    args = parser.parse_args()
    args.live_id = live_id(args.live_id)
    env = infra.read_env(infra.ENV_FILE)
    infra.validate_env(env)
    directory = ARTIFACTS / args.live_id
    if args.action == "live-demo":
        infra.preflight(env)
        with infra.driver_lock(), producer_lock():
            infra.assert_services()
            run_demo(args, env, directory)
    else:
        spec = {"live_id": args.live_id}
        if args.action == "live-stop":
            spec.update(json.loads((directory / "control.json").read_text()))
        result = invoke("stop-live" if args.action == "live-stop" else "live-status", spec, directory)
        atomic_json(directory / "status.json", result)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Owned cancellation requested; retain any unconfirmed marker before retry.", file=sys.stderr)
        sys.exit(130)
    except (ValueError, OSError, infra.InfraError) as error:
        print("ERROR: " + str(error), file=sys.stderr)
        sys.exit(1)
