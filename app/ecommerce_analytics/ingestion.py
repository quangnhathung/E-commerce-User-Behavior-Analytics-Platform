"""Distributed Bronze/Silver ingestion. Import only inside the Spark runtime."""
from datetime import timezone
import json
from pathlib import Path
import time

from pyspark import StorageLevel
from pyspark.sql import functions as F, types as T, Window

from .contracts import (SCHEMA_VERSION, SILVER_COLUMNS, SOURCE, TRANSFORM_VERSION,
                        fingerprint_sources, normalize_partition)
from .hdfs import Hdfs
from .snapshots import materialize, write_json


def silver_schema():
    return T.StructType([
        T.StructField("schema_version", T.IntegerType()),
        *[T.StructField(name, T.StringType()) for name in
          ("dataset_version", "source", "run_id", "event_id", "visitor_id", "item_id", "event_type", "transaction_id")],
        T.StructField("event_time", T.TimestampType()),
        T.StructField("event_date", T.DateType()),
        T.StructField("emitted_at", T.TimestampType()),
        T.StructField("source_file", T.StringType()),
        T.StructField("source_row_number", T.LongType()),
    ])


def normalized_schema():
    return T.StructType([*silver_schema().fields,
                         T.StructField("raw_line", T.StringType()),
                         T.StructField("error_codes", T.ArrayType(T.StringType())),
                         T.StructField("worker_host", T.StringType())])


def event_counts(frame):
    return {row["event_type"]: int(row["count"]) for row in
            frame.select(F.coalesce("event_type", F.lit("unknown")).alias("event_type"))
            .groupBy("event_type").count().collect()}


def iso_utc(value):
    return value.replace(tzinfo=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def transform_events(spark, storage, bronze, staging, version, *, require_two_workers=True):
    started = time.monotonic()
    # zipWithIndex preserves a physical source line number, including the header.
    records = spark.sparkContext.textFile(bronze + "/events.csv", minPartitions=4).zipWithIndex()
    normalized = spark.createDataFrame(records.mapPartitions(lambda rows: normalize_partition(rows, version)),
                                       normalized_schema()).persist(StorageLevel.DISK_ONLY)
    silver = None
    try:
        valid = normalized.where(F.size("error_codes") == 0)
        rejected = normalized.where(F.size("error_codes") > 0)
        overview = normalized.agg(F.count("*").alias("source_rows"),
                                  F.sum(F.when(F.size("error_codes") > 0, 1).otherwise(0)).alias("invalid_rows")).first()
        source_rows = int(overview.source_rows)
        invalid_rows = int(overview.invalid_rows or 0)
        hosts = {row.worker_host: int(row["count"]) for row in normalized.groupBy("worker_host").count().collect()}
        errors = {row.reason: int(row["count"]) for row in
                  rejected.select(F.explode("error_codes").alias("reason")).groupBy("reason").count().collect()}
        if source_rows == invalid_rows:
            print("INGEST_DIAGNOSTIC=" + json.dumps({"source_rows": source_rows, "invalid_rows": invalid_rows,
                                                      "error_counts": errors}), flush=True)
            raise ValueError("No valid events; Bronze is preserved but Silver will not be published")
        if require_two_workers and set(hosts) != {"spark-worker-1", "spark-worker-2"}:
            raise ValueError(f"Expected actual parsing on both Spark workers, got {sorted(hosts)}")
        duplicates = (valid.groupBy("event_id")
                      .agg(F.count("*").alias("occurrences"), F.min("source_row_number").alias("first_source_row_number"))
                      .where(F.col("occurrences") > 1))
        order = Window.partitionBy("event_id").orderBy("source_row_number")
        silver = (valid.withColumn("_position", F.row_number().over(order)).where(F.col("_position") == 1)
                  .select(*SILVER_COLUMNS).persist(StorageLevel.DISK_ONLY))
        stats = silver.agg(F.count("*").alias("rows"), F.countDistinct("visitor_id").alias("visitors"),
                           F.countDistinct("item_id").alias("items"),
                           F.countDistinct("transaction_id").alias("transactions"),
                           F.countDistinct(F.when(F.col("event_type") == "purchase", F.col("visitor_id"))).alias("buyers"),
                           F.countDistinct("event_date").alias("days"), F.min("event_time").alias("minimum"),
                           F.max("event_time").alias("maximum")).first()
        unique = int(stats.rows)
        duplicate_rows = source_rows - invalid_rows - unique
        if source_rows != unique + duplicate_rows + invalid_rows or duplicate_rows < 0:
            raise ValueError("Row reconciliation failed")
        silver.repartition(4, "event_date").write.mode("errorifexists").option("compression", "snappy").partitionBy("event_date").parquet(staging + "/events")
        rejected.select("source_file", "source_row_number", "raw_line", "error_codes").write.mode("errorifexists").option("compression", "snappy").parquet(staging + "/quarantine")
        duplicates.write.mode("errorifexists").option("compression", "snappy").parquet(staging + "/duplicates")
        quality = {
            "dataset_version": version, "schema_version": SCHEMA_VERSION, "transform_version": TRANSFORM_VERSION,
            "quality_status": "warning" if invalid_rows else "passed", "validation_scope": "events.csv",
            "source_rows": source_rows, "valid_rows_before_dedup": source_rows - invalid_rows,
            "silver_rows": unique, "duplicate_rows": duplicate_rows, "invalid_rows": invalid_rows,
            "error_counts": errors, "source_event_counts": event_counts(normalized),
            "silver_event_counts": event_counts(silver),
            "distinct_visitors": int(stats.visitors), "distinct_items": int(stats.items),
            "distinct_transactions": int(stats.transactions), "distinct_buyers": int(stats.buyers),
            "event_dates": int(stats.days), "min_event_time": iso_utc(stats.minimum), "max_event_time": iso_utc(stats.maximum),
            "worker_rows": hosts, "transform_seconds": round(time.monotonic() - started, 3),
        }
        write_json(storage, staging + "/quality.json", quality)
        return {"quality": quality, "silver_schema": silver_schema().jsonValue(),
                "events_directory": "events", "quarantine_directory": "quarantine", "duplicates_directory": "duplicates"}
    finally:
        if silver is not None:
            silver.unpersist()
        normalized.unpersist()


def verify_silver(spark, storage, root, manifest):
    quality = storage.read_json(root + "/quality.json")
    if quality != manifest["metadata"]["quality"]:
        raise ValueError("Silver report differs from the committed manifest")
    events = spark.read.parquet(root + "/events")
    expected = {field.name: field.dataType.simpleString() for field in silver_schema()}
    actual = {field.name: field.dataType.simpleString() for field in events.schema}
    if actual != expected or events.count() != quality["silver_rows"]:
        raise ValueError("Committed Silver schema/row count differs from the manifest")
    if quality["source_rows"] != quality["silver_rows"] + quality["duplicate_rows"] + quality["invalid_rows"]:
        raise ValueError("Committed quality report does not reconcile")
    if spark.read.parquet(root + "/quarantine").count() != quality["invalid_rows"]:
        raise ValueError("Quarantine row count differs from the manifest")
    duplicates = spark.read.parquet(root + "/duplicates")
    count = duplicates.agg(F.sum(F.col("occurrences") - 1)).first()[0] or 0
    if count != quality["duplicate_rows"]:
        raise ValueError("Duplicate audit does not reconcile")
    return quality


def run_ingest(spark, dataset_directory, code_sha256, *, base="/ecommerce", require_two_workers=True, fault=None):
    started = time.monotonic()
    storage = Hdfs(spark)
    print("INGEST_PROGRESS: fingerprinting four local source files", flush=True)
    source = fingerprint_sources(dataset_directory)
    version = source["dataset_version"]
    print("INGEST_PROGRESS: dataset_version=" + version, flush=True)
    bronze_path = base.rstrip("/") + "/bronze/retailrocket/" + version
    silver_path = base.rstrip("/") + "/silver/retailrocket/" + version + "/" + TRANSFORM_VERSION

    def build_bronze(staging):
        for entry in source["source_files"]:
            print(f"INGEST_PROGRESS: copy {entry['name']} ({entry['size']} bytes)", flush=True)
            storage.copy_from_local(str(Path(dataset_directory) / entry["name"]), staging + "/" + entry["name"])
        return {"source_files": source["source_files"]}

    bronze, bronze_reused = materialize(storage, bronze_path, {"kind": "bronze", "source": SOURCE, "dataset_version": version},
                                        build_bronze)
    if bronze["files"] != source["source_files"]:
        raise ValueError("Committed Bronze does not match the current local source fingerprint")
    if fault:
        fault("after_bronze")
    identity = {"kind": "silver", "source": SOURCE, "dataset_version": version,
                "transform_version": TRANSFORM_VERSION, "schema_version": SCHEMA_VERSION, "code_sha256": code_sha256}

    def build_silver(staging):
        print("INGEST_PROGRESS: parsing, normalizing and deduplicating events", flush=True)
        return transform_events(spark, storage, bronze_path, staging, version, require_two_workers=require_two_workers)

    silver, silver_reused = materialize(storage, silver_path, identity, build_silver,
                                        before_publish=(lambda: fault("before_silver_publish")) if fault else None)
    if fault:
        fault("after_silver_publish")
    quality = verify_silver(spark, storage, silver_path, silver)
    return {"dataset_version": version, "transform_version": TRANSFORM_VERSION, "quality_status": quality["quality_status"],
            "bronze_path": bronze_path, "silver_path": silver_path, "bronze_reused": bronze_reused, "silver_reused": silver_reused,
            "duration_seconds": round(time.monotonic() - started, 3),
            "bronze_manifest": bronze, "manifest": silver, "quality": quality}
