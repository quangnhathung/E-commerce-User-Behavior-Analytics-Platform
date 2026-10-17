"""Thin runtime dispatcher. The selected immutable ZIP supplies every application import."""
import argparse
import json
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("prepare-demo", "stream", "replay", "stream-status", "test-stream"))
    parser.add_argument("--spec-file", required=True)
    parser.add_argument("--code-archive", required=True)
    parser.add_argument("--code-sha256", required=True)
    args = parser.parse_args()
    sys.path.insert(0, args.code_archive)
    from ecommerce_analytics.contracts import sha256_file
    from ecommerce_streaming.store import Store
    if sha256_file(args.code_archive) != args.code_sha256:
        raise ValueError("Streaming code archive checksum mismatch")
    spec = json.loads(Path(args.spec_file).read_text())
    config = spec.get("config")
    if config is not None and config["code_sha256"] != args.code_sha256:
        raise ValueError("Run code fingerprint changed; restore code or prepare a new run")
    if args.action == "prepare-demo" and spec["request"]["code_sha256"] != args.code_sha256:
        raise ValueError("Prepared request has the wrong code fingerprint")
    store, spark = Store(), None
    try:
        if args.action == "stream-status":
            result = store.status(config)
        elif args.action == "replay":
            from ecommerce_streaming.kafka_io import replay
            result = replay(sys.stdin, config, store)
        else:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("replay-" + args.action).getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            if args.action == "prepare-demo":
                from ecommerce_streaming.prepare import prepare
                result = prepare(spark, spec["request"], spec["local_output"], store)
            elif args.action == "stream":
                from ecommerce_streaming.stream import run_stream
                result = run_stream(spark, config, store)
            else:
                from ecommerce_streaming.integration import run_tests
                result = run_tests(spark, store, args.code_sha256)
    finally:
        if spark is not None:
            spark.stop()
        store.close()
    print("STREAMING_RESULT=" + json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
