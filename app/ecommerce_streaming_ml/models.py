"""Validate frozen provenance separately from this run's inference archive."""
from datetime import timedelta

from ecommerce_ml.contracts import FEATURES, FEATURE_VERSION, MODEL_VERSION, PARAMS, coverage, digest, instant, iso
from ecommerce_ml.features import checked
from ecommerce_ml.native import NativeML
from ecommerce_ml.store import Store as SegmentStore
from ecommerce_prediction.contracts import MODEL_VERSION as PURCHASE_MODEL_VERSION
from ecommerce_purchase_policy.store import Store as PolicyStore
from ecommerce_purchase_policy.workflow import load_policy, model_source


def resolve(spark, storage, database, request, hashes):
    segment = SegmentStore(database).status(request["segment_model_version"])
    identity = segment["identity"]
    if (segment["model_version"] != request["segment_model_version"] or
            digest(identity) != segment["model_version"] or identity.get("kind") != "segments" or
            identity.get("model_version") != MODEL_VERSION or identity.get("parameters") != PARAMS or
            identity.get("code_sha256") != hashes["ml"] or identity.get("spark_version") != spark.version):
        raise ValueError("Segment model contract/code/Spark version differs from the frozen model")
    model = checked(storage, segment["model_path"], identity)
    if model["metadata"] != segment["report"] or storage.read_json(segment["model_path"] + "/quality.json") != segment["report"]:
        raise ValueError("Segment model publication changed")
    feature = checked(storage, identity["feature_path"])
    source_identity = feature["identity"]
    if (source_identity.get("kind") != "features" or digest(source_identity) != identity["feature_snapshot_id"] or
            storage.sha256(identity["feature_path"] + "/manifest.json") != identity["feature_manifest_sha256"] or
            source_identity.get("feature_version") != FEATURE_VERSION or source_identity.get("lookback_days") != 7 or
            source_identity.get("code_sha256") != hashes["ml"] or source_identity.get("as_of") != identity["as_of"]):
        raise ValueError("Segment feature source changed")
    policy_store = PolicyStore(database)
    purchase, data, index = model_source(spark, storage, policy_store, request["purchase_model_version"])
    policy, evaluation = load_policy(spark, storage, policy_store, request["policy_version"], hashes["purchase-policy"])
    if (purchase["model_version"] != request["purchase_model_version"] or policy["policy_version"] != request["policy_version"] or
            purchase["identity"].get("model_contract") != PURCHASE_MODEL_VERSION or
            purchase["identity"].get("code_sha256") != hashes["prediction"] or
            policy["model_version"] != purchase["model_version"] or
            policy["identity"]["model_manifest_sha256"] != purchase["report"]["model_manifest_sha256"] or
            policy["identity"]["data_manifest_sha256"] != purchase["identity"]["data_manifest_sha256"] or
            policy["identity"]["candidate_manifest_sha256"] != purchase["report"]["selection"]["winner"]["manifest_sha256"] or
            policy["identity"]["original_evaluation_manifest_sha256"] != purchase["report"]["evaluation_manifest_sha256"] or
            policy["identity"]["original_prediction_code_sha256"] != hashes["prediction"]):
        raise ValueError("Streaming policy and RF provenance differ")
    selection = purchase["report"]["selection"]
    if evaluation["old_threshold"] != selection["threshold"]:
        raise ValueError("Policy original RF threshold changed")
    if (source_identity.get("dataset_version") != request["dataset_version"] or
            source_identity.get("silver_path") != request["silver_path"] or
            data["identity"].get("dataset_version") != request["dataset_version"] or
            data["identity"].get("silver_path") != request["silver_path"] or
            data["identity"]["silver_manifest_sha256"] != source_identity.get("silver_manifest_sha256")):
        raise ValueError("Models must share the selected historical Silver snapshot")
    silver = checked(storage, request["silver_path"])
    if (silver["identity"].get("kind") != "silver" or silver["identity"].get("schema_version") != 1 or
            silver["identity"].get("transform_version") != "events-v1" or
            silver["identity"].get("source") != "retailrocket" or
            silver["identity"].get("dataset_version") != request["dataset_version"] or
            silver["identity"].get("code_sha256") != hashes["ingest"] or
            storage.sha256(request["silver_path"] + "/manifest.json") != source_identity["silver_manifest_sha256"]):
        raise ValueError("Streaming historical Silver source changed")
    quality = silver["metadata"]["quality"]
    if quality["quality_status"] != "passed":
        raise ValueError("Streaming ML requires passed historical Silver")
    coverage(request["start"], quality["min_event_time"], quality["max_event_time"])
    last_test = max(instant(entry["as_of"]) for entry in index["days"] if entry["split"] == "test")
    not_before = max(last_test + timedelta(days=1), instant(identity["as_of"]))
    if instant(request["start"]) < not_before:
        raise ValueError("START precedes the model/policy observation cutoff: " + iso(not_before))
    selected_k = segment["report"]["selected_k"]
    if type(selected_k) is not int or selected_k not in PARAMS["candidates"] or segment["report"]["feature_order"] != list(FEATURES):
        raise ValueError("Segment model feature order/K changed")
    return {"segment_model_version": segment["model_version"], "purchase_model_version": purchase["model_version"],
        "policy_version": policy["policy_version"], "segment_model_path": segment["model_path"],
        "purchase_model_path": purchase["model_path"], "segment_manifest_sha256": storage.sha256(segment["model_path"] + "/manifest.json"),
        "purchase_manifest_sha256": purchase["report"]["model_manifest_sha256"],
        "policy_selection_manifest_sha256": policy["report"]["selection_manifest_sha256"],
        "policy_evaluation_manifest_sha256": policy["report"]["evaluation_manifest_sha256"],
        "silver_manifest_sha256": source_identity["silver_manifest_sha256"], "selected_k": selected_k,
        "original_threshold": selection["threshold"], "threshold": evaluation["threshold"],
        "not_before": iso(not_before), "feature_version": FEATURE_VERSION, "feature_order": list(FEATURES),
        "frozen_hashes": hashes, "evaluation_kind": evaluation["evaluation_kind"], "fresh_holdout": False}


class Models:
    def __init__(self, spark, bindings):
        self.spark, self.bindings = spark, bindings
        native = NativeML(spark)
        self.segment = native.load(bindings["segment_model_path"] + "/pipeline")
        self.purchase = native.load(bindings["purchase_model_path"] + "/pipeline")

    def predict(self, features, scored_at):
        from pyspark.sql import functions as F
        from ecommerce_prediction.training import score_frame
        native, bindings = NativeML(self.spark), self.bindings
        segments = native.transform(self.segment, features).select("visitor_id", "cluster_id")
        values = score_frame(self.spark, self.purchase, features, threshold=bindings["threshold"])
        values = values.join(segments, "visitor_id").withColumn("original_predicted_label",
            (F.col("purchase_score") >= F.lit(bindings["original_threshold"])).cast("int"))
        for key, value in {"feature_version": FEATURE_VERSION, "scored_at": scored_at,
                **{key: bindings[key] for key in ("segment_model_version", "purchase_model_version", "policy_version",
                                                "threshold", "original_threshold")}}.items():
            values = values.withColumn(key, F.lit(value))
        from .contracts import PREDICTION_FIELDS
        return values.select(*PREDICTION_FIELDS)
