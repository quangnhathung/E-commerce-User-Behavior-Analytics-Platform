"""Pure contracts and an independent, bounded reference implementation."""
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import math
import re

from ecommerce_analytics.contracts import canonical_json

BEHAVIOR_VERSION = "behavior-v1"
SESSION_GAP_MS = 1_800_000
COUNT_FIELDS = ("event_count", "total_sessions", "view_sessions", "cart_sessions", "purchase_sessions")


def instant(value):
    value = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def iso(value):
    return instant(value).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def session_id(scope_id, visitor_id, first_event_id):
    return hashlib.sha256(canonical_json([BEHAVIOR_VERSION, scope_id, visitor_id, first_event_id]).encode()).hexdigest()


def counts(event_count, total_sessions, view_sessions, cart_sessions, purchase_sessions):
    values = dict(zip(COUNT_FIELDS, (event_count, total_sessions, view_sessions, cart_sessions, purchase_sessions)))
    if any(type(value) is not int or value < 0 for value in values.values()):
        raise ValueError("Funnel counts must be nonnegative integers")
    if not purchase_sessions <= cart_sessions <= view_sessions <= total_sessions <= event_count:
        raise ValueError("Funnel stage/session/event counts are inconsistent")
    ratio = lambda numerator, denominator: numerator / denominator if denominator else 0.0
    return {**values, "view_to_cart_rate": ratio(cart_sessions, view_sessions),
            "cart_to_purchase_rate": ratio(purchase_sessions, cart_sessions),
            "view_to_purchase_rate": ratio(purchase_sessions, view_sessions),
            "view_drop_count": view_sessions - cart_sessions, "cart_drop_count": cart_sessions - purchase_sessions,
            "view_drop_rate": ratio(view_sessions - cart_sessions, view_sessions),
            "cart_drop_rate": ratio(cart_sessions - purchase_sessions, cart_sessions)}


def validate_counts(row):
    expected = counts(**{key: row[key] for key in COUNT_FIELDS})
    for key, value in expected.items():
        actual = row.get(key)
        if isinstance(value, float):
            if not isinstance(actual, (float, int)) or not math.isclose(actual, value, rel_tol=0, abs_tol=1e-12):
                raise ValueError("Funnel rate differs from its counts: " + key)
        elif type(actual) is not int or actual != value:
            raise ValueError("Funnel count differs: " + key)


def product_fingerprint(rows):
    """Validate ordered rows and hash IDs/counts, independent of floating point formatting."""
    digest, count, previous = hashlib.sha256(), 0, None
    for row in rows:
        item = row["item_id"]
        if not isinstance(item, str) or not re.fullmatch(r"[0-9]+", item) or (previous is not None and item <= previous):
            raise ValueError("Funnel products must have unique string IDs in ascending order")
        validate_counts(row)
        digest.update((canonical_json([item, *[row[key] for key in COUNT_FIELDS]]) + "\n").encode())
        previous, count = item, count + 1
    return {"product_count": count, "products_sha256": digest.hexdigest()}


def reference(events, scope_id, *, max_events=10_000):
    """Small fixture/replay oracle only. Historical computation belongs to Spark."""
    visitors, seen = defaultdict(list), set()
    for event in events:
        if len(seen) >= max_events:
            raise ValueError("Behavior reference exceeds its bounded input limit")
        if event["event_id"] in seen:
            raise ValueError("Behavior input must already be deduplicated Silver")
        seen.add(event["event_id"])
        visitors[event["visitor_id"]].append({**event, "event_time": instant(event["event_time"])})
    sessions, items, products = [], [], defaultdict(lambda: dict.fromkeys(COUNT_FIELDS, 0))

    def finish(rows):
        visitor = rows[0]["visitor_id"]
        identity = session_id(scope_id, visitor, rows[0]["event_id"])
        start, end = rows[0]["event_time"], rows[-1]["event_time"]
        date = start.date().isoformat()
        grouped = defaultdict(list)
        for row in rows:
            grouped[row["item_id"]].append(row)
        reached = [0, 0, 0]
        for item, events in grouped.items():
            viewed = min((row["event_time"] for row in events if row["event_type"] == "view"), default=None)
            cart = min((row["event_time"] for row in events if row["event_type"] == "add_to_cart"
                        and viewed is not None and row["event_time"] > viewed), default=None)
            bought = min((row["event_time"] for row in events if row["event_type"] == "purchase"
                          and cart is not None and row["event_time"] > cart), default=None)
            flags = [int(value is not None) for value in (viewed, cart, bought)]
            items.append({"session_id": identity, "visitor_id": visitor, "item_id": item,
                          "session_start_date": date, "event_count": len(events),
                          "view_at": iso(viewed) if viewed else None, "cart_at": iso(cart) if cart else None,
                          "purchase_at": iso(bought) if bought else None})
            for key, value in zip(COUNT_FIELDS, (len(events), 1, *flags)):
                products[item][key] += value
            reached = [max(first, second) for first, second in zip(reached, flags)]
        sessions.append({"session_id": identity, "visitor_id": visitor, "session_start": iso(start),
                         "session_end": iso(end), "session_start_date": date, "event_count": len(rows),
                         **{name: sum(row["event_type"] == kind for row in rows) for name, kind in
                            (("views", "view"), ("add_to_cart", "add_to_cart"), ("purchase_events", "purchase"))},
                         **dict(zip(("view_reached", "cart_reached", "purchase_reached"), reached))})

    for rows in visitors.values():
        current = []
        for row in sorted(rows, key=lambda row: (row["event_time"], row["event_id"])):
            if current and row["event_time"] - current[-1]["event_time"] > timedelta(milliseconds=SESSION_GAP_MS):
                finish(current)
                current = []
            current.append(row)
        if current:
            finish(current)
    product_rows = [{"item_id": item, **counts(**value)} for item, value in sorted(products.items())]
    summary = {"behavior_version": BEHAVIOR_VERSION, "scope_id": scope_id,
               **counts(len(seen), len(sessions), *[sum(row[key] for row in sessions)
                        for key in ("view_reached", "cart_reached", "purchase_reached")]),
               **product_fingerprint(product_rows)}
    return {"summary": summary, "sessions": sorted(sessions, key=lambda row: row["session_id"]),
            "session_items": sorted(items, key=lambda row: (row["session_id"], row["item_id"])), "products": product_rows}
