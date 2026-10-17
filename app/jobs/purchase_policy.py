"""Checked archive entrypoint for policy evaluation and score relabeling."""
import argparse
import json
from pathlib import Path
import sys
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("tune-purchase-policy", "apply-purchase-policy", "purchase-policy-status", "test-purchase-policy"))
    parser.add_argument("--spec-file", required=True)
    parser.add_argument("--code-archive", required=True)
    parser.add_argument("--code-sha256", required=True)
    args = parser.parse_args()
    sys.path.insert(0, args.code_archive)
    from ecommerce_analytics.contracts import sha256_file
    from ecommerce_purchase_policy.store import Store
    if sha256_file(args.code_archive) != args.code_sha256:
        raise ValueError("Purchase policy archive checksum mismatch")
    spec = json.loads(Path(args.spec_file).read_text())
    if spec.get("code_sha256") != args.code_sha256:
        raise ValueError("Purchase policy specification code checksum mismatch")
    started, spark, store = time.monotonic(), None, None
    try:
        store = Store()
        if args.action == "purchase-policy-status":
            result = store.policy_status(spec["policy_version"])
        else:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName(args.action).getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            if spark.conf.get("spark.sql.session.timeZone") != "UTC":
                raise ValueError("Purchase policy requires UTC Spark SQL")
            if args.action == "tune-purchase-policy":
                from ecommerce_purchase_policy.workflow import tune
                result = tune(spark, store, spec["model_version"], args.code_sha256)
            elif args.action == "apply-purchase-policy":
                from ecommerce_purchase_policy.workflow import apply
                result = apply(spark, store, spec["policy_version"], spec["source_score_run_id"], args.code_sha256)
            else:
                from ecommerce_purchase_policy.integration import run_tests
                result = run_tests(spark, store, args.code_sha256)
    finally:
        if spark is not None:
            spark.stop()
        if store is not None:
            store.close()
    result["duration_seconds"] = round(time.monotonic() - started, 3)
    print("PURCHASE_POLICY_RESULT=" + json.dumps(result, sort_keys=True, allow_nan=False), flush=True)


if __name__ == "__main__":
    main()
