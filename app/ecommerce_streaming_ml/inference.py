"""Shared batch feature function applied only to seed plus accepted Kafka events."""
from ecommerce_analytics.snapshots import utc_now, write_json
from ecommerce_ml.contracts import FEATURES, COUNTS, fingerprint
from ecommerce_ml.features import feature_frame, rows
from .contracts import BINDING_IDS, PREDICTION_FIELDS, cutoff, prediction_fingerprint

EVENT_COLUMNS = ("visitor_id", "item_id", "event_type", "transaction_id", "event_time", "event_date")


def prediction_schema():
    from pyspark.sql import types as T
    kinds = {"visitor_id": T.StringType(), "as_of": T.TimestampType(), "last_event_time": T.TimestampType(),
        **{key: T.LongType() for key in COUNTS}, **{key: T.DoubleType() for key in FEATURES[5:]},
        "cluster_id": T.IntegerType(), "purchase_score": T.DoubleType(),
        "original_predicted_label": T.IntegerType(), "predicted_label": T.IntegerType(),
        "original_threshold": T.DoubleType(), "threshold": T.DoubleType(), "feature_version": T.StringType(),
        **{key: T.StringType() for key in BINDING_IDS}, "scored_at": T.StringType()}
    return T.StructType([T.StructField(key, kinds[key]) for key in PREDICTION_FIELDS])


def cohort_features(spark, config, accepted, as_of):
    from ecommerce_analytics.ingestion import silver_schema
    seed = spark.read.schema(silver_schema()).parquet(config["seed"]["path"])
    cohort = accepted.select("visitor_id").distinct()
    combined = seed.join(cohort, "visitor_id", "left_semi").select(*EVENT_COLUMNS).unionByName(accepted.select(*EVENT_COLUMNS))
    frame, _ = feature_frame(combined, as_of)
    return frame


def build(spark, storage, config, accepted, metadata, models, staging, previous_gold=None):
    from pyspark import StorageLevel
    bindings = config["bindings"]
    if not metadata["counts"]["accepted"] and previous_gold:
        previous = storage.read_json(previous_gold + "/ml-quality.json")
        spark.read.parquet(previous_gold + "/predictions").write.parquet(staging + "/predictions")
        spark.read.parquet(previous_gold + "/features").write.parquet(staging + "/features")
        quality = {**previous, "carried_forward": True}
    else:
        as_of, scored_at = cutoff(metadata["max_event_time"]), utc_now()
        if as_of is None:
            predictions = spark.createDataFrame([], prediction_schema())
            features = predictions.select("visitor_id", "as_of", "last_event_time", *FEATURES)
        else:
            features = cohort_features(spark, config, accepted, as_of)
            predictions = models.predict(features, scored_at)
        features = features.persist(StorageLevel.DISK_ONLY)
        predictions = predictions.persist(StorageLevel.DISK_ONLY)
        try:
            feature_hash = fingerprint(rows(features))
            predicted_hash = prediction_fingerprint(rows(predictions), bindings, as_of, scored_at)
            if predicted_hash["rows"] != feature_hash["rows"]:
                raise ValueError("Streaming model transform lost feature rows")
            features.write.mode("errorifexists").parquet(staging + "/features")
            predictions.write.mode("errorifexists").parquet(staging + "/predictions")
            quality = {"status": "ready" if predicted_hash["rows"] else "waiting_for_events", "as_of": as_of,
                "scored_at": scored_at, "fingerprint": predicted_hash, "feature_fingerprint": feature_hash,
                "carried_forward": False}
        finally:
            features.unpersist(); predictions.unpersist()
    write_json(storage, staging + "/ml-quality.json", quality)
    return quality


def verify(spark, storage, config, root):
    quality = storage.read_json(root + "/ml-quality.json")
    manifest = storage.read_json(root + "/manifest.json")
    if manifest["metadata"]["ml"] != quality:
        raise ValueError("Streaming ML quality differs from its manifest")
    predicted = spark.read.parquet(root + "/predictions")
    actual = prediction_fingerprint(rows(predicted), config["bindings"], quality["as_of"], quality["scored_at"])
    features = spark.read.parquet(root + "/features")
    if (actual != quality["fingerprint"] or fingerprint(rows(features)) != quality["feature_fingerprint"] or
            fingerprint(rows(predicted.select("visitor_id", "as_of", "last_event_time", *FEATURES))) != quality["feature_fingerprint"]):
        raise ValueError("Streaming ML Gold prediction/feature fingerprint changed")
    return quality, predicted


def reconcile(spark, storage, config, accepted, root, models):
    quality, _ = verify(spark, storage, config, root)
    if quality["status"] != "ready":
        raise ValueError("Completed streaming run has no accepted predictions")
    # Independent batch reconstruction over the exact source used by the streaming run.
    features = cohort_features(spark, config, accepted, quality["as_of"])
    if fingerprint(rows(features)) != quality["feature_fingerprint"]:
        raise ValueError("Final streaming features differ from batch reconstruction")
    actual = prediction_fingerprint(rows(models.predict(features, quality["scored_at"])), config["bindings"], quality["as_of"], quality["scored_at"])
    if actual != quality["fingerprint"]:
        raise ValueError("Final streaming predictions differ from batch transform")
    return quality
