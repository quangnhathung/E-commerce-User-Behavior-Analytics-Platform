"""Recoverable driver-side foreachBatch; parsing and analytics execute on Spark workers."""
from datetime import timedelta
import functools
import json
import time

from pyspark import StorageLevel
from pyspark.sql import functions as F, types as T, Window

from ecommerce_analytics.snapshots import materialize, write_json
from .contracts import (MAX_EVENTS, digest, dlq_record, instant, iso,
                        normalize_partition, source_offsets)
from .kafka_io import send_dlq
from .metrics import metrics


def silver_schema():
    return T.StructType([
        T.StructField("schema_version", T.IntegerType()),
        *[T.StructField(key, T.StringType()) for key in ("dataset_version", "event_id", "source", "visitor_id",
            "item_id", "event_type", "transaction_id", "run_id", "business_sha256")],
        T.StructField("event_time", T.TimestampType()), T.StructField("emitted_at", T.TimestampType()),
        T.StructField("event_date", T.DateType()), T.StructField("source_topic", T.StringType()),
        T.StructField("source_partition", T.IntegerType()), T.StructField("source_offset", T.LongType()),
        T.StructField("kafka_timestamp", T.TimestampType()), T.StructField("worker_host", T.StringType())])


def normalized_schema():
    return T.StructType([*silver_schema().fields, T.StructField("raw_key", T.BinaryType()),
                         T.StructField("raw_value", T.BinaryType()),
                         T.StructField("error_codes", T.ArrayType(T.StringType())),
                         T.StructField("foreign_run", T.BooleanType())])


def empty_events(spark):
    return spark.createDataFrame([], silver_schema())


def read_events(spark, roots):
    return spark.read.schema(silver_schema()).parquet(*[root + "/events" for root in roots]) if roots else empty_events(spark)


def classify(frame, previous, cutoff):
    previous_hashes = previous.select("event_id", F.col("business_sha256").alias("previous_hash"))
    valid = frame.where(~F.col("foreign_run") & (F.size("error_codes") == 0))
    order = Window.partitionBy("event_id").orderBy("source_topic", "source_partition", "source_offset")
    valid = (valid.join(previous_hashes, "event_id", "left")
             .withColumn("_rank", F.row_number().over(order))
             .withColumn("_first_hash", F.first("business_sha256").over(order)))
    conflict = ((F.col("previous_hash").isNotNull() & (F.col("previous_hash") != F.col("business_sha256"))) |
                (F.col("previous_hash").isNull() & (F.col("_first_hash") != F.col("business_sha256"))))
    duplicate = F.col("previous_hash").isNotNull() | (F.col("_rank") > 1)
    late = F.col("event_time") < F.lit(instant(cutoff) - timedelta(minutes=10)) if cutoff else F.lit(False)
    valid = valid.withColumn("category", F.when(conflict, "invalid").when(duplicate, "duplicate")
                            .when(late, "late").otherwise("accepted"))
    valid = valid.withColumn("error_codes", F.when(conflict, F.array(F.lit("EVENT_ID_CONFLICT")))
                            .when(F.col("category") == "late", F.array(F.lit("LATE_EVENT")))
                            .otherwise(F.col("error_codes")))
    columns = normalized_schema().fieldNames() + ["category"]
    invalid = frame.where(F.col("foreign_run") | (F.size("error_codes") > 0)).withColumn(
        "category", F.when(F.col("foreign_run"), "foreign").otherwise("invalid"))
    return valid.select(*columns).unionByName(invalid.select(*columns))


class Processor:
    def __init__(self, spark, storage, store, config, fault=None):
        self.spark, self.storage, self.store, self.config = spark, storage, store, config
        self.fault = fault

    def signal(self, stage):
        if self.fault:
            self.fault(stage)

    def __call__(self, incoming, batch_id):
        started = time.monotonic()
        config, storage, spark = self.config, self.storage, self.spark
        incoming = incoming.select("key", "value", "topic", "partition", "offset", "timestamp")
        groups = [row.asDict() for row in incoming.groupBy("topic", "partition").agg(
            F.min("offset").alias("minimum"), F.max("offset").alias("maximum"), F.count("*").alias("count"))
            .orderBy("topic", "partition").collect()]
        if not groups:
            return
        if sum(group["count"] for group in groups) > MAX_EVENTS:
            raise ValueError("Micro-batch exceeds the bounded demo limit")
        identity = {"run_config_sha256": digest(config), "batch_id": int(batch_id), "offsets": groups}
        suffix = f"/{batch_id:020d}"
        bronze_root = config["paths"]["bronze"] + suffix
        silver_root = config["paths"]["silver"] + suffix
        gold_root = config["paths"]["gold"] + suffix

        def build_bronze(staging):
            incoming.write.mode("errorifexists").option("compression", "snappy").parquet(staging + "/raw")
            return {"source_rows": sum(group["count"] for group in groups)}

        materialize(storage, bronze_root, {"kind": "stream-bronze", **identity}, build_bronze)
        self.signal("after_bronze")
        prior_roots = storage.batch_paths(config["paths"]["silver"], through=batch_id - 1)
        previous_metadata = storage.read_json(prior_roots[-1] + "/manifest.json")["metadata"] if prior_roots else None
        prior_offsets = previous_metadata["processed_offsets"] if previous_metadata else config["initial_offsets"]
        cutoff = previous_metadata["max_event_time"] if previous_metadata else None

        def build_silver(staging):
            raw = spark.read.parquet(bronze_root + "/raw")
            parser = functools.partial(normalize_partition, config=config)
            normalized = spark.createDataFrame(raw.rdd.mapPartitions(parser), normalized_schema()).persist(StorageLevel.DISK_ONLY)
            classified = None
            try:
                previous = read_events(spark, prior_roots)
                classified = classify(normalized, previous, cutoff).persist(StorageLevel.DISK_ONLY)
                categories = {row.category: int(row["count"]) for row in classified.groupBy("category").count().collect()}
                counts = {key: categories.get(key, 0) for key in ("accepted", "duplicate", "invalid", "late", "foreign")}
                raw_count = sum(group["count"] for group in groups)
                if sum(counts.values()) != raw_count:
                    raise ValueError("Streaming row reconciliation failed")
                accepted = classified.where(F.col("category") == "accepted").select(*silver_schema().fieldNames())
                if previous.count() + counts["accepted"] > config["event_count"]:
                    raise ValueError("Accepted events exceed the prepared run size")
                accepted.write.mode("errorifexists").option("compression", "snappy").parquet(staging + "/events")
                classified.where(F.col("category") != "accepted").write.mode("errorifexists").option("compression", "snappy").parquet(staging + "/audit")
                maximum = accepted.agg(F.max("event_time")).first()[0]
                maximum = max(filter(None, (cutoff, iso(maximum) if maximum else None)), default=None)
                workers = {row.worker_host: int(row["count"]) for row in normalized.groupBy("worker_host").count().collect()}
                errors = {row.reason: int(row["count"]) for row in classified.select(
                    F.explode("error_codes").alias("reason")).groupBy("reason").count().collect()}
                return {"source_rows": raw_count, "counts": counts, "error_counts": errors,
                        "worker_rows": workers, "max_event_time": maximum,
                        "processed_offsets": source_offsets(groups, prior_offsets)}
            finally:
                if classified is not None:
                    classified.unpersist()
                normalized.unpersist()

        silver, _ = materialize(storage, silver_root, {"kind": "stream-silver", **identity}, build_silver)
        self.signal("after_silver")

        def build_gold(staging):
            result = metrics(read_events(spark, prior_roots + [silver_root]))
            write_json(storage, staging + "/metrics.json", result)
            return {"processed_events": result["overview"]["processed_events"]}

        gold_identity = {"kind": "stream-gold", **identity, "silver_sha256": storage.sha256(silver_root + "/manifest.json")}
        materialize(storage, gold_root, gold_identity, build_gold)
        self.signal("after_gold")
        audit = spark.read.parquet(silver_root + "/audit").where(F.col("category").isin("invalid", "late"))
        send_dlq((dlq_record(row, config["run_id"], batch_id) for row in audit.toLocalIterator()), config["dlq_topic"])
        self.signal("after_dlq")
        gold = storage.read_json(gold_root + "/metrics.json")
        accepted = read_events(spark, [silver_root])
        # Samples are aggregate metadata, not source payload. A retry may republish
        # the same KPI; its latency timestamp describes that publication attempt.
        emitted_times = [iso(row.emitted_at) for row in accepted.select("emitted_at").toLocalIterator()]
        metadata = {**silver["metadata"], "bronze_path": bronze_root, "silver_path": silver_root,
                    "gold_path": gold_root, "publication_at": None, "latency_seconds": [],
                    "batch_seconds": round(time.monotonic() - started, 3)}
        published = self.store.publish(config, batch_id, gold, metadata, fault=self.fault)
        self.store.publication_timing(config, batch_id, published, emitted_times)
        print("STREAM_BATCH=" + json.dumps({"batch_id": batch_id,
              "counts": metadata["counts"], "processed_events": gold["overview"]["processed_events"]}), flush=True)
