"""Owned session control and a bounded ACK feed; business snapshots remain immutable."""
from ecommerce_streaming.contracts import digest, utc_now
from ecommerce_streaming_ml.store import Store
from .contracts import BUFFER, acknowledged_total


class LiveStore(Store):
    def ensure_session(self, request, owner):
        identity = request["live_id"]
        self.db.live_sessions.create_index([("base", 1), ("created_at", -1)])
        self.db.live_events.create_index([("live_id", 1), ("sequence", 1)], unique=True)
        self.db.live_sessions.update_one({"_id": identity}, {"$setOnInsert": {
            "live_id": identity, "base": request["base"], "request": request, "request_sha256": digest(request),
            "created_at": utc_now(), "completed_segments": 0, "completed_events": 0,
            "acknowledged_events": 0, "active_run_id": None, "last_snapshot_run_id": None,
            "segment_index": 0, "status": "starting"}}, upsert=True)
        previous = self.session(identity)
        if previous["request_sha256"] != digest(request) or previous["request"] != request:
            raise ValueError("LIVE_ID configuration/code changed; restore its version or choose a new LIVE_ID")
        self.db.live_sessions.update_one({"_id": identity}, {"$set": {
            "owner": owner, "stop_requested": False, "status": "starting", "heartbeat_at": utc_now()}})

    def session(self, identity, owner=None):
        row = self.db.live_sessions.find_one({"_id": identity})
        if row is None or (owner is not None and row.get("owner") != owner):
            raise ValueError("Live session ownership lost or missing")
        return row

    def state(self, identity, owner, **values):
        result = self.db.live_sessions.update_one({"_id": identity, "owner": owner}, {"$set": values})
        if result.matched_count != 1:
            raise ValueError("Live session ownership lost")

    def request_stop(self, identity, owner):
        self.state(identity, owner, stop_requested=True)
        return {"passed": True, "live_id": identity, "stop_requested": True}

    def ack(self, config, owner, seq, event, partition, next_offset, cursor):
        identity = config["live_id"]
        sequence = (config["segment_index"] - 1) * config["events_per_segment"] + seq
        self.db.live_events.update_one({"_id": f"{identity}:{sequence}"}, {"$setOnInsert": {
            "live_id": identity, "run_id": config["run_id"], "sequence": sequence,
            "segment_index": config["segment_index"], "event_id": event["event_id"],
            "visitor_id": event["visitor_id"], "item_id": event["item_id"], "event_type": event["event_type"],
            "event_time": event["event_time"], "emitted_at": event["emitted_at"],
            "partition": partition, "next_offset": next_offset, "acked_at": utc_now()}}, upsert=True)
        self.db.live_events.delete_many({"live_id": identity, "sequence": {"$lte": sequence - BUFFER}})
        self.producer_progress(config, cursor, "running")
        session = self.session(identity, owner)
        self.state(identity, owner, acknowledged_events=acknowledged_total(session["completed_segments"],
            config["events_per_segment"], config["segment_index"], seq))

    def finish_segment(self, config, owner, report):
        self.session(config["live_id"], owner)
        result = self.db.live_sessions.update_one({"_id": config["live_id"], "owner": owner,
            "completed_segments": config["segment_index"] - 1}, {"$set": {
                "completed_segments": config["segment_index"], "last_snapshot_run_id": config["run_id"],
                "acknowledged_events": config["segment_index"] * config["events_per_segment"],
                "status": "rotating"}, "$inc": {"completed_events": report["overview"]["processed_events"]}})
        if result.matched_count == 0 and self.session(config["live_id"], owner)["completed_segments"] != config["segment_index"]:
            raise ValueError("Unexpected live segment completion order")
