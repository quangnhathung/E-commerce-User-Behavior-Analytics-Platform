"""Dispatch purchase stages exclusively from the checked prediction archive."""
import argparse
import json
from pathlib import Path
import sys
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("prepare-purchase-data", "build-purchase-chunk", "finalize-purchase-data",
        "prepare-purchase-training", "train-purchase-candidate", "finish-purchase-training", "score-purchases",
        "purchase-status", "purchase-score-status", "test-purchase"))
    parser.add_argument("--spec-file", required=True)
    parser.add_argument("--code-archive", required=True)
    parser.add_argument("--code-sha256", required=True)
    args = parser.parse_args()
    sys.path.insert(0, args.code_archive)
    from ecommerce_analytics.contracts import sha256_file
    from ecommerce_prediction.store import Store
    if sha256_file(args.code_archive) != args.code_sha256:
        raise ValueError("Prediction archive checksum mismatch")
    spec = json.loads(Path(args.spec_file).read_text())
    if spec.get("code_sha256") != args.code_sha256:
        raise ValueError("Prediction specification code checksum mismatch")
    for key in ("config", "training"):
        if key in spec and spec[key]["identity"]["code_sha256"] != args.code_sha256:
            raise ValueError("Prediction stage code changed; use its original code or a new dataset")
    started, spark, store = time.monotonic(), None, None
    try:
        if args.action in ("purchase-status", "purchase-score-status"):
            store = Store()
            result = store.model_status(spec["model_version"]) if args.action == "purchase-status" else store.score_status(spec["score_run_id"], spec.get("visitor_id"))
        else:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName(args.action).getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            if spark.conf.get("spark.sql.session.timeZone") != "UTC":
                raise ValueError("Purchase processing requires UTC Spark SQL")
            if args.action == "prepare-purchase-data":
                from ecommerce_prediction.data import prepare
                result = prepare(spark, spec["request"])
            elif args.action == "build-purchase-chunk":
                from ecommerce_prediction.data import build_chunk
                result = build_chunk(spark, spec["config"], spec["entries"])
            elif args.action == "finalize-purchase-data":
                from ecommerce_prediction.data import finalize
                result = finalize(spark, spec["config"])
            else:
                store = Store()
                if args.action == "prepare-purchase-training":
                    from ecommerce_prediction.training import prepare_training
                    result = prepare_training(spark, store, spec["data"], args.code_sha256)
                elif args.action == "train-purchase-candidate":
                    from ecommerce_prediction.training import train_candidate
                    result = train_candidate(spark, spec["training"], spec["parameters"], args.code_sha256)
                elif args.action == "finish-purchase-training":
                    from ecommerce_prediction.training import finish_training
                    result = finish_training(spark, store, spec["training"], args.code_sha256)
                elif args.action == "score-purchases":
                    from ecommerce_prediction.scoring import score_batch
                    result = score_batch(spark, store, spec["request"], args.code_sha256)
                else:
                    from ecommerce_prediction.integration import run_tests
                    result = run_tests(spark, store, args.code_sha256)
    finally:
        if spark is not None:
            spark.stop()
        if store is not None:
            store.close()
    result["duration_seconds"] = round(time.monotonic() - started, 3)
    print("PREDICTION_RESULT=" + json.dumps(result, sort_keys=True, allow_nan=False), flush=True)


if __name__ == "__main__":
    main()
