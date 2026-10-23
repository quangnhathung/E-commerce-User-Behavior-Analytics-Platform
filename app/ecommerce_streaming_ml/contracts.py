"""Pure streaming inference contracts, scalar fingerprints and simulator source."""
from datetime import timedelta
import hashlib
import io
import json
import math
import socket
import zipfile

from ecommerce_analytics.contracts import canonical_json
from ecommerce_behavior.contracts import BEHAVIOR_VERSION
from ecommerce_ml.contracts import FEATURES, FEATURE_VERSION, canonical, identifier, normalize, visitor, iso
from ecommerce_streaming.contracts import (BUSINESS_FIELDS, MAX_EVENTS, business_hash, digest, instant,
    normalize_message as normalize_base, validate_request as validate_base)

PIPELINE_VERSION = "stream-ml-v1"
DEFAULT_START = "2015-09-18T00:00:00.000Z"
DEFAULT_END = "2015-09-18T03:00:00.000Z"
PROFILE_PACKAGES = {
    "ingest": ("ecommerce_analytics",),
    "streaming": ("ecommerce_analytics", "ecommerce_streaming", "ecommerce_behavior"),
    "ml": ("ecommerce_analytics", "ecommerce_ml"),
    "prediction": ("ecommerce_analytics", "ecommerce_ml", "ecommerce_prediction"),
    "purchase-policy": ("ecommerce_analytics", "ecommerce_ml", "ecommerce_prediction", "ecommerce_purchase_policy"),
}
BINDING_IDS = ("segment_model_version", "purchase_model_version", "policy_version")
PREDICTION_FIELDS = ("visitor_id", "as_of", "last_event_time", *FEATURES, "cluster_id", "purchase_score",
    "original_predicted_label", "predicted_label", "original_threshold", "threshold", "feature_version",
    *BINDING_IDS, "scored_at")


def frozen_hashes(archive):
    """Recreate old ZIP profiles from this ZIP, with exactly the packager's metadata."""
    result = {}
    with zipfile.ZipFile(archive) as source:
        for profile, packages in PROFILE_PACKAGES.items():
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as output:
                for name in sorted(source.namelist()):
                    if name.endswith(".py") and name.split("/", 1)[0] in packages:
                        entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                        entry.compress_type = zipfile.ZIP_DEFLATED
                        entry.external_attr = 0o644 << 16
                        output.writestr(entry, source.read(name))
            result[profile] = hashlib.sha256(buffer.getvalue()).hexdigest()
    return result


def validate_request(request):
    if request.get("pipeline_version") != PIPELINE_VERSION:
        raise ValueError("Unsupported streaming ML contract")
    validate_base({**request, "pipeline_version": "stream-v2"})
    if instant(request["end"]) - instant(request["start"]) > timedelta(days=1):
        raise ValueError("Bounded streaming ML demos support at most 24 hours per run")
    if request.get("demo_source") not in ("replay", "simulator"):
        raise ValueError("DEMO_SOURCE must be replay or simulator")
    for key in BINDING_IDS:
        identifier(request[key])
    if not request.get("base", "").startswith("/") or ".." in request["base"].split("/"):
        raise ValueError("Streaming ML base must be an absolute HDFS directory")
    if request["demo_source"] == "simulator" and instant(request["end"]) <= instant(request["start"]) + timedelta(minutes=2, seconds=2):
        raise ValueError("Simulator END must include its three phases (at least 123 seconds)")


def paths(run_id, base="/ecommerce"):
    base = base.rstrip("/")
    return {name: f"{base}/{name}/streaming-ml/{run_id}" for name in ("bronze", "silver", "gold")} | {
        "prepared": f"{base}/streaming-ml/{run_id}", "checkpoint": f"{base}/checkpoints/streaming-ml/{run_id}"}


def normalize_message(message, config, host):
    # Keep every existing wire validation. Only the explicitly selected simulator source differs.
    value = normalize_base(message, config, host)
    if config["demo_source"] == "simulator":
        if value["source"] == "simulator":
            value["error_codes"] = [code for code in value["error_codes"] if code != "INVALID_SOURCE"]
        elif not value["foreign_run"] and "INVALID_SOURCE" not in value["error_codes"]:
            value["error_codes"].append("INVALID_SOURCE")
    if not value["foreign_run"] and not value["error_codes"]:
        value["business_sha256"] = business_hash(json.loads(bytes(message["value"]).decode()))
    return value


def normalize_partition(messages, config):
    host = socket.gethostname()
    for message in messages:
        yield normalize_message(message.asDict(), config, host)


def simulator_events(request):
    start = instant(request["start"])
    ids = ("990000000001", "990000000002", "000099000000003")
    phases = [
        [(ids[0], "10", "view", None), (ids[1], "20", "view", None), (ids[2], "30", "add_to_cart", None)],
        [(ids[0], "10", "add_to_cart", None), (ids[1], "21", "view", None), (ids[2], "30", "view", None)],
        [(ids[0], "10", "purchase", "990000001"), (ids[1], "22", "view", None), (ids[2], "30", "purchase", "990000002")],
    ]
    result = []
    for phase, entries in enumerate(phases):
        for index, (who, item, kind, transaction) in enumerate(entries):
            value = {"schema_version": 1, "dataset_version": request["dataset_version"], "source": "simulator",
                "visitor_id": who, "item_id": item, "event_type": kind, "transaction_id": transaction,
                "event_time": iso(start + timedelta(minutes=phase, seconds=index))}
            value["event_id"] = digest([PIPELINE_VERSION, request["run_id"], value])
            result.append(value)
    return result


def cutoff(maximum):
    return iso(instant(maximum) + timedelta(milliseconds=1)) if maximum is not None else None


def normalized_prediction(row, bindings, as_of, scored_at):
    value = normalize(row)
    if value["as_of"] != as_of or iso(row["scored_at"]) != scored_at:
        raise ValueError("Prediction cutoff/publication timestamp differs from Gold")
    for key in BINDING_IDS:
        if row[key] != bindings[key]:
            raise ValueError("Prediction model/policy binding changed")
    if row["feature_version"] != FEATURE_VERSION:
        raise ValueError("Prediction feature contract changed")
    cluster, score = row["cluster_id"], row["purchase_score"]
    if type(cluster) is not int or not 0 <= cluster < bindings["selected_k"]:
        raise ValueError("Invalid streaming cluster")
    if isinstance(score, bool) or not isinstance(score, (int, float)) or not math.isfinite(score) or not 0 <= score <= 1:
        raise ValueError("Invalid purchase score")
    for label, threshold_key in (("original_predicted_label", "original_threshold"), ("predicted_label", "threshold")):
        threshold = bindings[threshold_key]
        if row[threshold_key] != threshold or type(row[label]) is not int or row[label] != int(score >= threshold):
            raise ValueError("Prediction label/threshold mismatch")
    return {**value, **{key: row[key] for key in PREDICTION_FIELDS if key not in value}, "scored_at": scored_at}


def prediction_fingerprint(rows, bindings, as_of, scored_at):
    checksum, scores, previous, count, positive, original = hashlib.sha256(), hashlib.sha256(), None, 0, 0, 0
    clusters = {str(index): 0 for index in range(bindings["selected_k"])}
    for row in rows:
        value = normalized_prediction(row, bindings, as_of, scored_at)
        who = value["visitor_id"]
        if previous is not None and who <= previous:
            raise ValueError("Predictions must have unique ordered visitor IDs")
        scalar = [*canonical(value), value["cluster_id"], float(value["purchase_score"]).hex(),
                  value["original_predicted_label"], value["predicted_label"]]
        checksum.update((canonical_json(scalar) + "\n").encode())
        scores.update((canonical_json([who, float(value["purchase_score"]).hex()]) + "\n").encode())
        clusters[str(value["cluster_id"])] += 1
        previous, count = who, count + 1
        positive += value["predicted_label"]
        original += value["original_predicted_label"]
        if count > MAX_EVENTS:
            raise ValueError("Streaming prediction cohort exceeds the bounded run")
    return {"rows": count, "sha256": checksum.hexdigest(), "scores_sha256": scores.hexdigest(),
            "predicted_positive": positive, "original_predicted_positive": original, "clusters": clusters}
