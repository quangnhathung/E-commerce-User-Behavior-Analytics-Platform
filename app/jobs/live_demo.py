"""Private runtime entrypoint; the HTTP API never starts or stops jobs."""
import argparse
import json
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("live-demo", "stop-live", "live-status"))
    parser.add_argument("--spec-file", required=True)
    parser.add_argument("--code-archive", required=True)
    parser.add_argument("--code-sha256", required=True)
    args = parser.parse_args()
    sys.path.insert(0, args.code_archive)
    from ecommerce_analytics.contracts import sha256_file
    from ecommerce_live.contracts import validate
    from ecommerce_live.store import LiveStore
    from ecommerce_streaming_ml.contracts import frozen_hashes
    spec = json.loads(Path(args.spec_file).read_text())
    if sha256_file(args.code_archive) != args.code_sha256:
        raise ValueError("Live archive checksum mismatch")
    store, spark = LiveStore(), None
    try:
        if args.action == "stop-live":
            result = store.request_stop(spec["live_id"], spec["owner"])
        elif args.action == "live-status":
            session = store.session(spec["live_id"])
            result = {key: session.get(key) for key in ("live_id", "status", "heartbeat_at", "active_run_id",
                "completed_segments", "completed_events", "acknowledged_events", "stop_requested")}
        else:
            request = spec["request"]
            validate(request)
            if request["code_sha256"] != args.code_sha256:
                raise ValueError("Live code changed; restore its version or use a new LIVE_ID")
            from pyspark.sql import SparkSession
            from ecommerce_live.runtime import run
            spark = SparkSession.builder.appName("live-demo-" + request["live_id"]).getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            result = run(spark, store, request, frozen_hashes(args.code_archive), spec["owner"])
    finally:
        if spark is not None:
            spark.stop()
        store.close()
    print("LIVE_RESULT=" + json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
