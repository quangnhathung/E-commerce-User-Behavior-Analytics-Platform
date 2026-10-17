"""Distributed seven-day aggregates and immutable feature snapshots."""
from datetime import timedelta

from ecommerce_analytics.hdfs import Hdfs
from ecommerce_analytics.snapshots import materialize, verify_snapshot, verify_replicas, write_json
from .contracts import FEATURE_VERSION, FEATURES, COUNTS, coverage, digest, fingerprint, iso, window


def checked(storage, root, identity=None):
    original = storage.read_json(root + "/manifest.json")
    result = verify_snapshot(storage, root, identity or original["identity"])
    verify_replicas(storage, root, result["files"])
    return result


def feature_frame(events, as_of):
    from pyspark.sql import functions as F
    start, end = window(as_of)
    source = events.where((F.col("event_date") >= F.lit(start.date())) &
        (F.col("event_date") <= F.lit((end - timedelta(milliseconds=1)).date())) &
        (F.col("event_time") >= F.lit(start)) & (F.col("event_time") < F.lit(end)))
    features = source.groupBy("visitor_id").agg(
        F.sum((F.col("event_type") == "view").cast("long")).alias(COUNTS[0]),
        F.sum((F.col("event_type") == "add_to_cart").cast("long")).alias(COUNTS[1]),
        F.countDistinct(F.when(F.col("event_type") == "purchase", F.col("transaction_id"))).alias(COUNTS[2]),
        F.countDistinct("item_id").alias(COUNTS[3]), F.countDistinct("event_date").alias(COUNTS[4]),
        F.max("event_time").alias("last_event_time"))
    features = features.withColumn("as_of", F.lit(end)).withColumn("recency_hours",
        (F.unix_millis("as_of") - F.unix_millis("last_event_time")) / F.lit(3_600_000.0))
    for key, numerator in ((FEATURES[6], COUNTS[1]), (FEATURES[7], COUNTS[2])):
        features = features.withColumn(key, F.when(F.col(COUNTS[0]) > 0,
            F.col(numerator) / F.col(COUNTS[0])).otherwise(F.lit(0.0)))
    return features.select("visitor_id", "as_of", "last_event_time", *FEATURES), source


def rows(frame):
    # Only one sorted partition is buffered at a time, never the entire cohort.
    for row in frame.orderBy("visitor_id").toLocalIterator():
        yield row.asDict()


def validate_frame(frame, as_of):
    from pyspark.sql import functions as F
    expected = {"visitor_id": "string", "as_of": "timestamp", "last_event_time": "timestamp",
                **{key: "bigint" for key in COUNTS}, **{key: "double" for key in FEATURES[5:]}}
    if {field.name: field.dataType.simpleString() for field in frame.schema} != expected:
        raise ValueError("Feature schema differs from features-v1")
    if frame.where(F.col("as_of").isNull() | (F.col("as_of") != F.lit(window(as_of)[1]))).limit(1).count():
        raise ValueError("Feature AS_OF differs from its pinned identity")
    result = fingerprint(rows(frame))
    if not result["rows"]:
        raise ValueError("The seven-day feature window is empty")
    return result


def load_features(spark, storage, root, *, snapshot_id=None):
    manifest = checked(storage, root)
    identity = manifest["identity"]
    if identity.get("kind") != "features" or identity.get("feature_version") != FEATURE_VERSION or identity.get("lookback_days") != 7:
        raise ValueError("Unsupported feature identity/version")
    actual_id = digest(identity)
    if snapshot_id is not None and actual_id != snapshot_id:
        raise ValueError("Feature snapshot ID differs from its manifest")
    frame = spark.read.parquet(root + "/features")
    quality = validate_frame(frame, identity["as_of"])
    if quality != manifest["metadata"]["fingerprint"] or storage.read_json(root + "/quality.json") != manifest["metadata"]:
        raise ValueError("Feature content differs from its saved quality report")
    return frame, manifest


def build_features(spark, request, *, fault=None):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    from ecommerce_analytics.ingestion import silver_schema
    storage = Hdfs(spark)
    if request["feature_version"] != FEATURE_VERSION:
        raise ValueError("Unsupported feature version")
    source_root = request["silver_path"]
    source = checked(storage, source_root)
    if any(source["identity"].get(key) != value for key, value in {
        "kind": "silver", "source": "retailrocket", "dataset_version": request["dataset_version"],
        "schema_version": 1, "transform_version": "events-v1"}.items()):
        raise ValueError("ML source differs from checked historical Silver")
    quality = source["metadata"]["quality"]
    coverage(request["as_of"], quality["min_event_time"], quality["max_event_time"])
    identity = {"kind": "features", "feature_version": FEATURE_VERSION, "lookback_days": 7,
        "as_of": request["as_of"], "dataset_version": request["dataset_version"],
        "silver_path": source_root, "silver_manifest_sha256": storage.sha256(source_root + "/manifest.json"),
        "code_sha256": request["code_sha256"]}
    snapshot_id = digest(identity)
    destination = request["base"].rstrip("/") + "/gold/features/" + snapshot_id

    def build(staging):
        events = spark.read.parquet(source_root + "/events")
        if {f.name: f.dataType.simpleString() for f in events.schema} != {
                f.name: f.dataType.simpleString() for f in silver_schema()}:
            raise ValueError("Silver schema differs from events-v1")
        frame, selected = feature_frame(events, request["as_of"])
        frame = frame.persist(StorageLevel.DISK_ONLY)
        try:
            stats = validate_frame(frame, request["as_of"])
            counts = selected.groupBy("event_type").count().collect()  # At most three rows.
            event_counts = {row["event_type"]: row["count"] for row in counts}
            if stats["sums"][COUNTS[0]] != event_counts.get("view", 0) or stats["sums"][COUNTS[1]] != event_counts.get("add_to_cart", 0):
                raise ValueError("Feature event reconciliation failed")
            metadata = {"passed": True, "fingerprint": stats, "feature_order": list(FEATURES),
                "event_count": sum(event_counts.values()), "event_counts": event_counts,
                "zero_view_visitors": frame.where(F.col(COUNTS[0]) == 0).count(),
                "visitors_with_transactions": frame.where(F.col(COUNTS[2]) > 0).count(),
                "max_recency_hours": frame.agg(F.max("recency_hours")).first()[0]}
            frame.write.mode("errorifexists").parquet(staging + "/features")
            write_json(storage, staging + "/quality.json", metadata)
            return metadata
        finally:
            frame.unpersist()

    manifest, reused = materialize(storage, destination, identity, build,
        before_publish=(lambda: fault("before_feature_gold")) if fault else None)
    if fault:
        fault("after_feature_gold")
    load_features(spark, storage, destination, snapshot_id=snapshot_id)
    return {"passed": True, "feature_snapshot_id": snapshot_id, "feature_path": destination,
            "manifest": manifest, "manifest_sha256": storage.sha256(destination + "/manifest.json"),
            "quality": manifest["metadata"], "reused": reused, "minimum_block_replicas": 2}
