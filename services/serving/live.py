"""Read-only live session/feed views; ACK telemetry never substitutes for Spark KPIs."""
from datetime import datetime

from ecommerce_streaming.contracts import digest
from ecommerce_live.contracts import acknowledged_total
from .contracts import ServingError, now


def public(repo, row):
    if row.get("request_sha256") != digest(row["request"]):
        raise ServingError(503, "LIVE_CONFIG_CHANGED", "Cấu hình phiên live không hợp lệ")
    request = row["request"]
    active = repo.one("stream_runs", {"_id": row.get("active_run_id"), "config.base": "/ecommerce"}) if row.get("active_run_id") else None
    snapshot = active if active and active["committed_batch_id"] >= 0 else (
        repo.one("stream_runs", {"_id": row["last_snapshot_run_id"], "config.base": "/ecommerce"}) if row.get("last_snapshot_run_id") else None)
    # Snapshot coordinates come from the actual atomic run pointer, not the controller heartbeat.
    batch = repo.one("stream_batches", {"_id": f"{active['run_id']}:{active['committed_batch_id']}"}) if active and active["committed_batch_id"] >= 0 else None
    overview = repo.one("kpi_overview", {"_id": f"{active['run_id']}:{active['committed_batch_id']}"}) if batch else None
    completed = row["completed_segments"]
    current_events = overview["processed_events"] if overview and row["segment_index"] > completed else 0
    status = row["status"]
    heartbeat = row.get("heartbeat_at")
    if status in ("starting", "preparing", "running", "rotating"):
        if not heartbeat or (datetime.fromisoformat(now().replace("Z", "+00:00")) -
                datetime.fromisoformat(heartbeat.replace("Z", "+00:00"))).total_seconds() > 20:
            status = "stale"
    return {"live_id": row["live_id"], "status": status, "heartbeat_at": heartbeat,
        "active_run_id": row.get("active_run_id"), "segment_index": row["segment_index"],
        "completed_segments": completed, "acknowledged_events": acknowledged_total(completed, request["events_per_segment"],
            row["segment_index"], active["producer"]["last_seq"] if active else -1),
        "processed_events": row["completed_events"] + current_events, "stop_requested": row.get("stop_requested", False),
        "snapshot_run_id": snapshot["run_id"] if snapshot else None,
        "snapshot_batch_id": snapshot["committed_batch_id"] if snapshot else -1,
        "processed_offsets": batch["processed_offsets"] if batch else {},
        "start": request["start"], "end": request["end"], "rate": request["rate"],
        "events_per_segment": request["events_per_segment"], "observed_at": now()}


def session(repo, identity):
    row = repo.one("live_sessions", {"_id": identity, "base": "/ecommerce"})
    if row is None:
        raise ServingError(404, "LIVE_NOT_FOUND", "Không tìm thấy phiên live")
    return public(repo, row)


def sessions(repo):
    return {"data": [public(repo, row) for row in repo.rows("live_sessions", {"base": "/ecommerce"},
        [("created_at", -1)], 20)], "observed_at": now()}


def processed(event, state):
    return event["segment_index"] <= state["completed_segments"] or (
        event["run_id"] == state["active_run_id"] and
        event["next_offset"] <= state["processed_offsets"].get(event["partition"], -1))


def events(repo, identity, after, limit):
    state = session(repo, identity)
    oldest = next(iter(repo.rows("live_events", {"live_id": identity}, [("sequence", 1)], 1)), None)
    gap = after is not None and oldest is not None and after < oldest["sequence"] - 1
    query = {"live_id": identity}
    tail = after is None or gap
    if not tail:
        query["sequence"] = {"$gt": after}
    rows = list(repo.rows("live_events", query, [("sequence", -1 if tail else 1)], limit))
    if tail:
        rows.reverse()
    keys = ("sequence", "run_id", "segment_index", "event_id", "visitor_id", "item_id", "event_type",
            "event_time", "emitted_at", "acked_at", "partition", "next_offset")
    values = [{**{key: row[key] for key in keys}, "processing_status": "processed" if processed(row, state) else "acked"} for row in rows]
    return {"data": values, "session": state, "gap": gap,
            "next_after": rows[-1]["sequence"] if rows else after, "observed_at": now()}
