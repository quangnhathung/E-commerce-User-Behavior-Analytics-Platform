"""ML runtime dispatcher; imports exclusively from the checked immutable ZIP."""
import argparse
import json
from pathlib import Path
import sys
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("build-features", "train-segments", "segment-status", "test-ml"))
    parser.add_argument("--spec-file", required=True)
    parser.add_argument("--code-archive", required=True)
    parser.add_argument("--code-sha256", required=True)
    args = parser.parse_args()
    sys.path.insert(0, args.code_archive)
    from ecommerce_analytics.contracts import sha256_file
    from ecommerce_ml.store import Store
    if sha256_file(args.code_archive) != args.code_sha256:
        raise ValueError("ML code archive checksum mismatch")
    spec = json.loads(Path(args.spec_file).read_text())
    if "request" in spec and spec["request"]["code_sha256"] != args.code_sha256:
        raise ValueError("ML request code fingerprint mismatch")
    started = time.monotonic()
    store, spark = None, None
    try:
        if args.action == "segment-status":
            store = Store()
            result = store.status(spec["model_version"], spec.get("visitor_id"))
        else:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("historical-" + args.action).getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            if spark.conf.get("spark.sql.session.timeZone") != "UTC":
                raise ValueError("ML requires a UTC Spark SQL session")
            if args.action == "build-features":
                from ecommerce_ml.features import build_features
                result = build_features(spark, spec["request"])
            elif args.action == "train-segments":
                from ecommerce_ml.training import train_segments
                store = Store()
                result = train_segments(spark, store, spec["request"])
            else:
                from ecommerce_ml.integration import run_tests
                store = Store()
                result = run_tests(spark, store, args.code_sha256)
    finally:
        if spark is not None:
            spark.stop()
        if store is not None:
            store.close()
    result["duration_seconds"] = round(time.monotonic() - started, 3)
    print("ML_RESULT=" + json.dumps(result, sort_keys=True, allow_nan=False), flush=True)


if __name__ == "__main__":
    main()
