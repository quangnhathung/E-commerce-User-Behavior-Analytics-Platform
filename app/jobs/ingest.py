"""spark-submit entry point. Dependencies arrive via --py-files."""
import argparse
import json
from pyspark.sql import SparkSession
from ecommerce_analytics.contracts import sha256_file
from ecommerce_analytics.ingestion import run_ingest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default="/opt/dataset")
    parser.add_argument("--code-archive", required=True)
    parser.add_argument("--code-sha256", required=True)
    args = parser.parse_args()
    if sha256_file(args.code_archive) != args.code_sha256:
        raise ValueError("Application archive checksum changed before submission")
    spark = SparkSession.builder.appName("retailrocket-ingest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = run_ingest(spark, args.dataset, args.code_sha256)
        print("INGEST_RESULT=" + json.dumps(result, sort_keys=True), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
