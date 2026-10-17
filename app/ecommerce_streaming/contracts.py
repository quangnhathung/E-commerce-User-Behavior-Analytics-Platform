"""Pure, versioned wire and run contracts; no Spark/Kafka/Mongo imports."""
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import re
import socket

from ecommerce_analytics.contracts import canonical_json
from ecommerce_behavior.contracts import BEHAVIOR_VERSION

PIPELINE_VERSION = "stream-v2"
MAX_EVENTS = 10_000
DEFAULT_START = "2015-09-10T10:00:00.000Z"
DEFAULT_END = "2015-09-10T16:00:00.000Z"
BUSINESS_FIELDS = ("schema_version", "dataset_version", "event_id", "source", "visitor_id",
                   "item_id", "event_type", "transaction_id", "event_time")
HEX = re.compile(r"[0-9a-f]{64}\Z")


def utc_now():
    return iso(datetime.now(timezone.utc))


def iso(value):
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def instant(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z", value):
        raise ValueError("Expected UTC timestamp YYYY-MM-DDTHH:MM:SS.mmmZ")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def digest(value):
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def business_hash(payload):
    return digest([payload.get(key) for key in BUSINESS_FIELDS])


def validate_request(request):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", request["run_id"]):
        raise ValueError("RUN_ID must contain 1..64 letters, digits, underscores or hyphens")
    if instant(request["start"]) >= instant(request["end"]):
        raise ValueError("START must be earlier than END")
    rate = request["rate"]
    if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate) or not 0 < rate <= 10:
        raise ValueError("RATE must be greater than zero and at most 10 events/second")
    if request["pipeline_version"] != PIPELINE_VERSION:
        raise ValueError("Unsupported pipeline version")
    if request.get("behavior_version") != BEHAVIOR_VERSION:
        raise ValueError("Unsupported behavior version")
    if not HEX.fullmatch(request["code_sha256"]) or not HEX.fullmatch(request["dataset_version"]):
        raise ValueError("Invalid source/code fingerprint")


def paths(run_id, base="/ecommerce"):
    base = base.rstrip("/")
    return {name: f"{base}/{name}/replay/{run_id}" for name in ("bronze", "silver", "gold")} | {
        "prepared": f"{base}/replay/{run_id}", "checkpoint": f"{base}/checkpoints/replay/{run_id}"}


def normalize_message(message, config, host):
    result = {key: None for key in (*BUSINESS_FIELDS, "run_id", "emitted_at", "event_date", "business_sha256")}
    result.update(source_topic=message["topic"], source_partition=message["partition"],
                  source_offset=message["offset"], kafka_timestamp=message["timestamp"],
                  raw_key=message["key"], raw_value=message["value"], worker_host=host,
                  error_codes=[], foreign_run=False)
    errors = result["error_codes"]
    try:
        payload = json.loads(bytes(message["value"]).decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Expected object")
    except (ValueError, TypeError, UnicodeDecodeError):
        errors.append("MALFORMED_JSON")
        return result
    if isinstance(payload.get("run_id"), str) and payload["run_id"] != config["run_id"]:
        result["foreign_run"] = True
        return result
    # Copy only values of the right type into the explicit Spark schema.
    for field in BUSINESS_FIELDS[1:] + ("run_id",):
        value = payload.get(field)
        if isinstance(value, str):
            result[field] = value
    if type(payload.get("schema_version")) is int:
        result["schema_version"] = payload["schema_version"]
    if result["schema_version"] != 1:
        errors.append("INVALID_SCHEMA_VERSION")
    if result["run_id"] != config["run_id"]:
        errors.append("INVALID_RUN_ID")
    if result["dataset_version"] != config["dataset_version"]:
        errors.append("INVALID_DATASET_VERSION")
    if result["source"] != "retailrocket":
        errors.append("INVALID_SOURCE")
    if not HEX.fullmatch(result["event_id"] or ""):
        errors.append("INVALID_EVENT_ID")
    for field in ("visitor_id", "item_id"):
        if not re.fullmatch(r"[0-9]+", result[field] or ""):
            errors.append("INVALID_" + field.upper())
    if result["event_type"] not in ("view", "add_to_cart", "purchase"):
        errors.append("UNKNOWN_EVENT_TYPE")
    transaction = payload.get("transaction_id")
    if "transaction_id" not in payload or (transaction is not None and
            (not isinstance(transaction, str) or not re.fullmatch(r"[0-9]+", transaction))):
        errors.append("INVALID_TRANSACTION_ID")
    elif result["event_type"] == "purchase" and transaction is None:
        errors.append("MISSING_TRANSACTION_ID")
    for field in ("event_time", "emitted_at"):
        try:
            result[field] = instant(payload.get(field))
        except (ValueError, TypeError):
            result[field] = None
            errors.append("INVALID_" + field.upper())
    if result["event_time"] is not None:
        result["event_date"] = result["event_time"].date()
        if not instant(config["start"]) <= result["event_time"] < instant(config["end"]):
            errors.append("OUTSIDE_REPLAY_RANGE")
    if not errors:
        result["business_sha256"] = business_hash(payload)
    return result


def normalize_partition(messages, config):
    host = socket.gethostname()
    for message in messages:
        yield normalize_message(message.asDict(), config, host)


def source_offsets(rows, initial):
    offsets = dict(initial)
    for row in rows:
        partition = str(row["partition"])
        offsets[partition] = max(offsets.get(partition, 0), int(row["maximum"]) + 1)
    return offsets


def drained(processed, target):
    return all(int(processed.get(partition, -1)) >= int(offset) for partition, offset in target.items())


def dlq_record(row, run_id, batch_id):
    coordinates = [row.source_topic, row.source_partition, row.source_offset]
    return {"dlq_id": digest(coordinates), "run_id": run_id, "batch_id": batch_id,
            "topic": row.source_topic, "partition": row.source_partition, "offset": row.source_offset,
            "error_codes": row.error_codes, "raw_key_base64": base64.b64encode(row.raw_key or b"").decode(),
            "raw_value_base64": base64.b64encode(row.raw_value or b"").decode()}


def reference_metrics(events):
    """Independent, bounded reference used by tests (production uses Spark)."""
    def counts(rows):
        visitors = {row["visitor_id"] for row in rows}
        buyers = {row["visitor_id"] for row in rows if row["event_type"] == "purchase"}
        return {"processed_events": len(rows), "views": sum(row["event_type"] == "view" for row in rows),
                "add_to_cart": sum(row["event_type"] == "add_to_cart" for row in rows),
                "purchase_events": sum(row["event_type"] == "purchase" for row in rows),
                "transactions": len({row["transaction_id"] for row in rows if row["transaction_id"] is not None}),
                "visitors": len(visitors), "buyers": len(buyers),
                "buyer_rate": len(buyers) / len(visitors) if visitors else 0.0}
    overview = counts(events)
    latest = max((instant(row["event_time"]) for row in events), default=None)
    overview.update(max_event_time=iso(latest) if latest else None,
                    active_visitors=len({row["visitor_id"] for row in events
                        if latest - timedelta(minutes=5) < instant(row["event_time"]) <= latest}) if latest else 0)
    groups = {}
    for row in events:
        minute = iso(instant(row["event_time"]).replace(second=0, microsecond=0))
        groups.setdefault(minute, []).append(row)
    return {"overview": overview, "minutes": [{"window_start": minute, **counts(rows)}
                                              for minute, rows in sorted(groups.items())]}
