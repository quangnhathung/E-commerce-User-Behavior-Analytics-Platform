"""Reproducible varied traffic; no runtime or third-party dependencies."""
from datetime import timedelta
import random
import re
import math

from ecommerce_streaming.contracts import digest, instant, iso
from ecommerce_streaming_ml.contracts import validate_request

VERSION = "live-v1"
BUFFER = 2000
MAX_INFLIGHT = 200
ID_PATTERN = r"[A-Za-z0-9][A-Za-z0-9_-]{0,39}"


def live_id(value):
    if not isinstance(value, str) or not re.fullmatch(ID_PATTERN, value):
        raise ValueError("LIVE_ID requires 1..40 letters, digits, underscores or hyphens")
    return value


def validate(request):
    live_id(request["live_id"])
    if request.get("live_version") != VERSION or not re.fullmatch(r"[0-9a-f]{32}", request.get("seed", "")):
        raise ValueError("Invalid live version/seed")
    for key, minimum, maximum in (("events_per_segment", 12, 1000), ("visitors", 3, 200),
                                   ("items", 3, 100), ("max_segments", 0, 1000000)):
        if type(request.get(key)) is not int or not minimum <= request[key] <= maximum:
            raise ValueError("Invalid live parameter: " + key)
    if request["visitors"] > request["events_per_segment"] // 2:
        raise ValueError("Allow at least two events per visitor")
    if not isinstance(request.get("minimum_disk_gib"), (int, float)) or isinstance(request["minimum_disk_gib"], bool) or not math.isfinite(request["minimum_disk_gib"]) or request["minimum_disk_gib"] <= 0:
        raise ValueError("Invalid live disk reserve")
    child = child_request(request, 1)
    validate_request(child)


def child_request(request, segment):
    return {**request, "run_id": f"{request['live_id']}-{segment:06d}", "segment_index": segment,
            "demo_source": "simulator", "simulator_profile": VERSION}


def events(request):
    """Finite source sealed before sending; random choices are pinned by seed/segment."""
    rng = random.Random(digest([VERSION, request["seed"], request["segment_index"]]))
    namespace = int(digest([request["seed"], request["live_id"], request["segment_index"]])[:24], 16)
    visitors = [("0000" if i == 0 else "") + str(namespace + i) for i in range(request["visitors"])]
    journeys = {who: {"intent": rng.choices(["browse", "abandon", "buy"], [.55, .25, .20])[0],
                       "item": str(rng.randint(1, request["items"])), "stage": "view", "views": 0}
                for who in visitors}
    weights = [rng.choice([1, 1, 2, 3, 8]) for _ in visitors]
    start = instant(request["start"])
    span_ms = min(int((instant(request["end"]) - start).total_seconds() * 1000) - 1, request["events_per_segment"] * 1000)
    result = []
    arrived = 0
    for seq in range(request["events_per_segment"]):
        remaining = len(visitors) - arrived
        if not arrived or request["events_per_segment"] - seq <= remaining or (remaining and rng.random() < .25):
            who = visitors[arrived]
            arrived += 1
        else:
            who = rng.choices(visitors[:arrived], weights[:arrived])[0]
        state = journeys[who]
        kind, transaction = "view", None
        if state["intent"] != "browse" and state["views"] and state["stage"] == "view" and rng.random() < .35:
            kind, state["stage"] = "add_to_cart", "cart"
        elif state["intent"] == "buy" and state["stage"] == "cart" and rng.random() < .5:
            kind, transaction, state["stage"] = "purchase", str(namespace * 10000 + seq), "purchased"
        else:
            if state["stage"] == "purchased" or state["intent"] == "browse" or rng.random() < .2:
                state["item"], state["stage"] = str(rng.randint(1, request["items"])), "view"
            state["views"] += 1
        value = {"schema_version": 1, "dataset_version": request["dataset_version"], "source": "simulator",
                 "visitor_id": who, "item_id": state["item"], "event_type": kind, "transaction_id": transaction,
                 "event_time": iso(start + timedelta(milliseconds=seq * span_ms // request["events_per_segment"]))}
        value["event_id"] = digest([VERSION, request["run_id"], seq, value])
        result.append(value)
    return result


def acknowledged_total(completed_segments, events_per_segment, segment_index, last_seq):
    return completed_segments * events_per_segment + (last_seq + 1 if segment_index > completed_segments else 0)
