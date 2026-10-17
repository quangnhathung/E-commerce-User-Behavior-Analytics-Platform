"""Portable feature contract and bounded fixture oracle; no Spark dependency."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import re

FEATURE_VERSION = "features-v1"
MODEL_VERSION = "segments-v1"
DEFAULT_AS_OF = "2015-07-15T00:00:00.000Z"
FEATURES = ("view_count_7d", "cart_count_7d", "transaction_count_7d", "unique_items_7d",
            "active_days_7d", "recency_hours", "cart_view_ratio_7d", "transaction_view_ratio_7d")
COUNTS = FEATURES[:5]
PARAMS = {"candidates": [2, 3, 4, 5], "seed": 42, "initMode": "k-means||", "initSteps": 2,
          "maxIter": 30, "tol": 1e-4, "distanceMeasure": "euclidean", "solver": "row",
          "silhouette_distance": "squaredEuclidean", "withMean": True, "withStd": True,
          "log1p": list(COUNTS), "feature_order": list(FEATURES)}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def instant(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z", value):
        raise ValueError("AS_OF/timestamp must be UTC with milliseconds: YYYY-MM-DDTHH:MM:SS.sssZ")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def iso(value):
    if isinstance(value, str):
        instant(value)
        return value
    if not isinstance(value, datetime) or value.microsecond % 1000:
        raise ValueError("Feature timestamp must have millisecond precision")
    return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc).isoformat(
        timespec="milliseconds").replace("+00:00", "Z")


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("Snapshot/model ID must be 64 lowercase hexadecimal characters")
    return value


def visitor(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]+", value):
        raise ValueError("VISITOR_ID must be a nonempty digit string")
    return value


def window(as_of):
    end = instant(as_of)
    return end - timedelta(days=7), end


def coverage(as_of, minimum, maximum):
    start, end = window(as_of)
    if start < instant(minimum) or end > instant(maximum) + timedelta(milliseconds=1):
        raise ValueError("The full seven-day window is outside the observed Silver range")


def derived(as_of, last, views, carts, transactions):
    delta = instant(as_of) - instant(last)
    milliseconds = (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000
    return {"recency_hours": milliseconds / 3_600_000,
            "cart_view_ratio_7d": carts / views if views else 0.0,
            "transaction_view_ratio_7d": transactions / views if views else 0.0}


def normalize(row):
    row = dict(row)
    visitor(row["visitor_id"])
    row["as_of"], row["last_event_time"] = iso(row["as_of"]), iso(row["last_event_time"])
    start, end = window(row["as_of"])
    if not start <= instant(row["last_event_time"]) < end:
        raise ValueError("Feature last event is outside its window")
    for key in COUNTS:
        if isinstance(row[key], bool) or not isinstance(row[key], int) or row[key] < 0:
            raise ValueError("Feature counts must be nonnegative integers")
    calendar_days = ((end - timedelta(milliseconds=1)).date() - start.date()).days + 1
    if not 1 <= row["active_days_7d"] <= calendar_days or row["unique_items_7d"] < 1:
        raise ValueError("Feature activity is inconsistent")
    if not sum(row[key] for key in COUNTS[:3]):
        raise ValueError("An active visitor must have at least one counted event")
    expected = derived(row["as_of"], row["last_event_time"], *(row[key] for key in COUNTS[:3]))
    for key, value in expected.items():
        if isinstance(row[key], bool) or not isinstance(row[key], (int, float)) or not math.isfinite(row[key]) or row[key] < 0 or abs(row[key] - value) > 1e-12:
            raise ValueError("Nonfinite or inconsistent derived feature: " + key)
    return {key: row[key] for key in ("visitor_id", "as_of", "last_event_time", *FEATURES)}


def canonical(row):
    value = normalize(row)
    # Derived doubles are validated, then represented by their exact integer inputs.
    return [value["visitor_id"], value["as_of"], value["last_event_time"], *[value[key] for key in COUNTS]]


def fingerprint(rows, *, assignments=False):
    checksum, previous, count = hashlib.sha256(), None, 0
    sums = {key: 0 for key in COUNTS}
    for row in rows:
        value = canonical(row)
        if previous is not None and value[0] <= previous:
            raise ValueError("Feature rows must have unique visitor IDs in lexical order")
        previous = value[0]
        if assignments:
            cluster = row["cluster_id"]
            if isinstance(cluster, bool) or not isinstance(cluster, int) or cluster < 0:
                raise ValueError("Invalid cluster ID")
            value.append(cluster)
        checksum.update((json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode())
        count += 1
        for key in COUNTS:
            sums[key] += row[key]
    return {"rows": count, "sha256": checksum.hexdigest(), "sums": sums}


def reference(events, as_of=DEFAULT_AS_OF, *, limit=100_000):
    """Only for bounded fixtures. Input is normalized, deduplicated Silver events."""
    start, end = window(as_of)
    groups = {}
    for index, event in enumerate(events):
        if index >= limit:
            raise ValueError("Reference input exceeds the small-fixture limit")
        timestamp = instant(iso(event["event_time"]))
        if start <= timestamp < end:
            visitor(event["visitor_id"])
            groups.setdefault(event["visitor_id"], []).append((event, timestamp))
    result = []
    for key, values in sorted(groups.items()):
        views = sum(event["event_type"] == "view" for event, _ in values)
        carts = sum(event["event_type"] == "add_to_cart" for event, _ in values)
        transactions = len({event["transaction_id"] for event, _ in values
                            if event["event_type"] == "purchase" and event.get("transaction_id") is not None})
        last = iso(max(timestamp for _, timestamp in values))
        result.append(normalize({"visitor_id": key, "as_of": as_of, "last_event_time": last,
            "view_count_7d": views, "cart_count_7d": carts, "transaction_count_7d": transactions,
            "unique_items_7d": len({event["item_id"] for event, _ in values}),
            "active_days_7d": len({timestamp.date() for _, timestamp in values}),
            **derived(as_of, last, views, carts, transactions)}))
    return result


def choose(candidates):
    valid = [value for value in candidates if value.get("valid") and
             math.isfinite(value["silhouette"]) and -1 <= value["silhouette"] <= 1]
    if not valid:
        raise ValueError("No valid K-Means candidate; no model was published")
    best = max(value["silhouette"] for value in valid)
    return min((value for value in valid if best - value["silhouette"] <= 1e-12), key=lambda value: value["k"])
