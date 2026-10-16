"""Exercise Kafka -> Structured Streaming -> HDFS -> executor-side MongoDB writes."""
import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F, types as T

from common import consume, create_topic, delete_topic, mongo, mongo_settings, publish
from readiness import check


def executor_hostname(rows):
    import socket
    import time
    list(rows)
    time.sleep(1)
    yield socket.gethostname()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true", help="Keep test artifacts for inspection")
    args = parser.parse_args()
    status = check()
    run_id = "infra-smoke-" + uuid.uuid4().hex
    topic = run_id
    base = os.environ["HDFS_URI"] + "/ecommerce/_smoke/" + run_id
    output = base + "/events"
    checkpoint = base + "/checkpoint"
    database = os.environ["MONGO_DATABASE"]
    spark = None
    created = False
    succeeded = False
    cleanup_errors = []
    try:
        create_topic(topic)
        created = True
        now = datetime.now(timezone.utc).isoformat()
        records = [{"schema_version": 1, "run_id": run_id, "event_id": str(index),
                    "visitor_id": f"smoke-{index}", "item_id": "test-item",
                    "event_type": "view", "source": "infra-smoke",
                    "event_time": now, "emitted_at": now, "transaction_id": None}
                   for index in range(6)]
        publish(topic, records)
        if {row["event_id"] for row in consume(topic, 6)} != {str(i) for i in range(6)}:
            raise RuntimeError("Kafka round-trip content mismatch")
        spark = SparkSession.builder.appName(run_id).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        hosts = set(spark.sparkContext.parallelize(range(8), 8).mapPartitions(executor_hostname).collect())
        if hosts != {"spark-worker-1", "spark-worker-2"}:
            raise RuntimeError(f"Expected execution on both workers, got {sorted(hosts)}")
        total = spark.range(1_000_000, numPartitions=8).agg(F.sum("id")).first()[0]
        if total != 499999500000:
            raise RuntimeError("Distributed aggregation returned an incorrect result")
        schema = T.StructType([
            T.StructField("schema_version", T.IntegerType()),
            *[T.StructField(name, T.StringType()) for name in
              ("run_id", "event_id", "visitor_id", "item_id", "event_type", "source", "event_time", "emitted_at", "transaction_id")],
        ])
        source = (spark.readStream.format("kafka")
                  .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
                  .option("subscribe", topic).option("startingOffsets", "earliest").load())
        events = source.select(F.from_json(F.col("value").cast("string"), schema).alias("event")).select("event.*")
        for iteration in range(2):
            query = (events.writeStream.format("parquet").option("path", output)
                     .option("checkpointLocation", checkpoint).trigger(availableNow=True).start())
            if not query.awaitTermination(120):
                query.stop()
                raise RuntimeError("Streaming smoke test exceeded 120 seconds")
            actual = spark.read.parquet(output)
            if actual.count() != 6 or {row.event_id for row in actual.select("event_id").collect()} != {str(i) for i in range(6)}:
                raise RuntimeError(f"Parquet mismatch after checkpoint run {iteration + 1}")
        settings = mongo_settings()

        def write_partition(rows):
            from pymongo import MongoClient
            with MongoClient(**settings) as client:
                for row in rows:
                    record = row.asDict()
                    record["_id"] = run_id + ":" + record["event_id"]
                    client[database].infra_smoke.replace_one({"_id": record["_id"]}, record, upsert=True)

        for _ in range(2):
            actual.repartition(4).foreachPartition(write_partition)
        with mongo() as client:
            rows = list(client[database].infra_smoke.find({"run_id": run_id}))
            if len(rows) != 6 or {row["event_id"] for row in rows} != {str(i) for i in range(6)}:
                raise RuntimeError("MongoDB executor write/read mismatch or duplicate upserts")
        filesystem = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        if not filesystem.exists(spark._jvm.org.apache.hadoop.fs.Path(checkpoint + "/commits")):
            raise RuntimeError("Streaming checkpoint commit log missing on HDFS")
        block_replicas = []
        for file in filesystem.listStatus(spark._jvm.org.apache.hadoop.fs.Path(output)):
            if file.isFile() and file.getPath().getName().startswith("part-"):
                for block in filesystem.getFileBlockLocations(file, 0, file.getLen()):
                    block_replicas.append(len(set(block.getHosts())))
        if not block_replicas or min(block_replicas) != 2:
            raise RuntimeError("Parquet data blocks do not have two live HDFS replicas")
        result = {**status, "run_id": run_id, "records": 6, "executor_hosts": sorted(hosts),
                  "aggregation_sum": total, "checkpoint_restart": True, "mongo_upsert_retry": True,
                  "minimum_block_replicas": min(block_replicas),
                  "hdfs_path": base, "kept": args.keep}
        succeeded = True
    finally:
        if not args.keep:
            if spark is not None:
                try:
                    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
                    fs.delete(spark._jvm.org.apache.hadoop.fs.Path(base), True)
                except Exception as error:
                    cleanup_errors.append(type(error).__name__ + ": HDFS cleanup failed")
            try:
                with mongo() as client:
                    client[database].infra_smoke.delete_many({"run_id": run_id})
            except Exception as error:
                cleanup_errors.append(type(error).__name__ + ": MongoDB cleanup failed")
        if spark is not None:
            spark.stop()
        if created and not args.keep:
            try:
                delete_topic(topic)
            except Exception as error:
                cleanup_errors.append(type(error).__name__ + ": Kafka cleanup failed")
        for message in cleanup_errors:
            print(message, file=sys.stderr)
    if succeeded:
        if cleanup_errors:
            raise RuntimeError("Smoke assertions passed but resource cleanup failed")
        print("INFRA_RESULT=" + json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
