"""Runtime entrypoint for an isolated streaming ML archive."""
import argparse
import json
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("prepare", "stream", "replay", "status", "test"))
    parser.add_argument("--spec-file", required=True)
    parser.add_argument("--code-archive", required=True)
    parser.add_argument("--code-sha256", required=True)
    args = parser.parse_args()
    sys.path.insert(0, args.code_archive)
    from ecommerce_analytics.contracts import sha256_file
    from ecommerce_streaming_ml.contracts import frozen_hashes
    from ecommerce_streaming_ml.store import Store
    if sha256_file(args.code_archive) != args.code_sha256:
        raise ValueError("Streaming ML archive checksum mismatch")
    spec = json.loads(Path(args.spec_file).read_text())
    config = spec.get("config")
    if config is not None and config["code_sha256"] != args.code_sha256:
        raise ValueError("Run code fingerprint changed; restore code or prepare a new ML run")
    if args.action == "prepare" and spec["request"]["code_sha256"] != args.code_sha256:
        raise ValueError("Streaming ML preparation code fingerprint changed")
    hashes = frozen_hashes(args.code_archive)
    store, spark = Store(), None
    try:
        if args.action == "status":
            result = store.status(config, spec.get("visitor_id"))
        elif args.action == "replay":
            from ecommerce_streaming_ml.producer import replay
            result = replay(sys.stdin, config, store)
        else:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("streaming-ml-" + args.action).getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            if args.action == "prepare":
                from ecommerce_streaming_ml.prepare import prepare
                result = prepare(spark, spec["request"], spec["local_output"], store, hashes)
            elif args.action == "stream":
                from ecommerce_streaming_ml.stream import run_stream
                result = run_stream(spark, config, store, hashes)
            else:
                from ecommerce_streaming_ml.integration import run_tests
                result = run_tests(spark, store, args.code_sha256, hashes)
    finally:
        if spark is not None:
            spark.stop()
        store.close()
    print("STREAM_ML_RESULT=" + json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
