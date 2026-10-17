"""Batch scores use only history before AS_OF and a published locked model."""
from ecommerce_analytics.hdfs import Hdfs
from ecommerce_analytics.snapshots import materialize, write_json
from ecommerce_ml.features import checked, feature_frame, validate_frame
from ecommerce_ml.native import NativeML
from .contracts import (AUDIT, FEATURES, MODEL_VERSION, SCORE_VERSION, coverage, digest, instant, row_fingerprint)
from .data import KNOWN_SOURCE, fire, ordered_rows, source
from .metrics import validate_scores
from .training import score_frame


def score_batch(spark, store, request, code_sha256, *, fault=None):
    from pyspark import StorageLevel
    storage = Hdfs(spark)
    record = store.model_status(request["model_version"])
    model = checked(storage, record["model_path"], record["identity"])
    if record["identity"].get("model_contract") != MODEL_VERSION or record["identity"].get("feature_order") != list(FEATURES):
        raise ValueError("Unsupported purchase model contract/features")
    if storage.sha256(record["model_path"] + "/manifest.json") != record["report"]["model_manifest_sha256"]:
        raise ValueError("Published purchase model manifest changed")
    if record["report"]["selection"] != model["metadata"]:
        raise ValueError("Mongo purchase selection differs from its locked HDFS model")
    if record["identity"]["code_sha256"] != code_sha256 or record["identity"]["spark_version"] != spark.version:
        raise ValueError("Purchase scoring requires the model's exact prediction code/Spark version")
    data_manifest = checked(storage, record["data_path"])
    data = data_manifest["identity"]["config"]
    if data_manifest["identity"]["kind"] != "purchase-index" or data["purchase_dataset_id"] != record["identity"]["purchase_dataset_id"]:
        raise ValueError("Published purchase model data identity changed")
    if storage.sha256(record["data_path"] + "/manifest.json") != record["identity"]["data_manifest_sha256"]:
        raise ValueError("Published purchase data manifest changed")
    first_test = min(entry["as_of"] for entry in data["identity"]["schedule"] if entry["split"] == "test")
    if instant(request["as_of"]) < instant(first_test):
        raise ValueError("Batch scoring AS_OF must be at or after the first test cutoff")
    events, source_manifest, source_hash = source(spark, storage, data["identity"], data["identity"]["silver_manifest_sha256"])
    quality = source_manifest["metadata"]["quality"]
    coverage(request["as_of"], quality["min_event_time"], quality["max_event_time"])
    threshold = model["metadata"]["threshold"]
    identity = {"kind": "purchase-score", "score_version": SCORE_VERSION, "as_of": request["as_of"],
        "model_version": request["model_version"], "model_manifest_sha256": record["report"]["model_manifest_sha256"],
        "silver_manifest_sha256": source_hash, "silver_path": data["identity"]["silver_path"],
        "code_sha256": code_sha256, "threshold": threshold}
    key = digest(identity)
    path = data["base"] + "/gold/purchase-scores/" + key
    store.prepare_scores(identity, path, storage.exists(path))
    def predicted():
        features, _ = feature_frame(events, request["as_of"])
        validate_frame(features, request["as_of"])
        return score_frame(spark, NativeML(spark).load(record["model_path"] + "/pipeline"), features, threshold=threshold)
    def build(staging):
        frame = predicted().persist(StorageLevel.DISK_ONLY)
        try:
            validate_scores(frame, labeled=False)
            fingerprint = row_fingerprint(ordered_rows(frame), scored=True, threshold=threshold)
            if data["identity"]["dataset_version"] == KNOWN_SOURCE and request["as_of"] == "2015-09-18T00:00:00.000Z" and fingerprint["rows"] != 69052:
                raise ValueError("Default scoring cohort differs from the CSV baseline")
            report = {"passed": True, "as_of": request["as_of"], "threshold": threshold,
                      "fingerprint": fingerprint, "reload_verified": True, "feature_order": list(FEATURES)}
            frame.write.mode("errorifexists").parquet(staging + "/scores")
            write_json(storage, staging + "/quality.json", report)
            return report
        finally:
            frame.unpersist()
    manifest, reused = materialize(storage, path, identity, build,
        before_publish=lambda: fire(fault, "before_purchase_score_gold"))
    fire(fault, "after_purchase_score_gold")
    frame = spark.read.parquet(path + "/scores")
    expected = manifest["metadata"]["fingerprint"]
    if row_fingerprint(ordered_rows(frame), scored=True, threshold=threshold) != expected:
        raise ValueError("Saved purchase scores changed")
    # Re-load model even on the first run and prove complete cohort score identity.
    if row_fingerprint(ordered_rows(predicted()), scored=True, threshold=threshold) != expected:
        raise ValueError("Reloaded model changes batch purchase scores")
    if storage.read_json(path + "/quality.json") != manifest["metadata"]:
        raise ValueError("Purchase score quality metadata changed")
    store.publish_scores(identity, path, manifest["metadata"], ordered_rows(frame), fault=fault)
    return {"passed": True, "score_run_id": key, "model_version": request["model_version"], "score_path": path,
            "manifest": manifest, "quality": manifest["metadata"], "reused": reused, "minimum_block_replicas": 2}
