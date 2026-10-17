"""Immutable full-history behavior snapshots, reusing checked ingestion outputs."""
import time

from ecommerce_analytics.snapshots import materialize
from ecommerce_streaming.contracts import digest, utc_now
from ecommerce_streaming.storage import Storage
from .contracts import BEHAVIOR_VERSION
from .store import BehaviorStore


def analyze(spark, store, request, *, fault=None):
    from .pipeline import product_rows, write_behavior
    started = time.monotonic()
    storage = Storage(spark)
    if request["behavior_version"] != BEHAVIOR_VERSION:
        raise ValueError("Unsupported behavior version")
    source_root = request["silver_path"]
    original = storage.read_json(source_root + "/manifest.json")
    source_identity = original["identity"]
    if any(source_identity.get(key) != value for key, value in {
            "kind": "silver", "source": "retailrocket", "dataset_version": request["dataset_version"],
            "schema_version": 1, "transform_version": "events-v1"}.items()):
        raise ValueError("Behavior source differs from the historical Silver contract")
    storage.checked(source_root, source_identity)
    identity = {"kind": "behavior-history", "request": request,
                "silver_manifest_sha256": storage.sha256(source_root + "/manifest.json")}
    analysis_id = digest(identity)
    scope_id = "history:" + analysis_id
    destination = request["base"].rstrip("/") + "/gold/behavior/history/" + analysis_id
    behavior = BehaviorStore(store.db)
    behavior.indexes()
    previous = store.db.behavior_runs.find_one({"_id": analysis_id})
    if previous is not None and previous["identity"] != identity:
        raise ValueError("Historical behavior registry identity mismatch")
    if previous is not None and previous.get("status") == "published" and not storage.exists(destination):
        raise ValueError("Published historical Gold is missing; restore the snapshot")
    store.db.behavior_runs.update_one({"_id": analysis_id}, {"$setOnInsert": {
        "analysis_id": analysis_id, "identity": identity, "status": "prepared", "created_at": utc_now()}}, upsert=True)

    def build(staging):
        from ecommerce_analytics.ingestion import silver_schema
        source = spark.read.parquet(source_root + "/events")
        if {field.name: field.dataType.simpleString() for field in source.schema} != {
                field.name: field.dataType.simpleString() for field in silver_schema()}:
            raise ValueError("Historical Silver schema differs from events-v1")
        summary = write_behavior(source, storage, staging, scope_id)
        if summary["event_count"] != original["metadata"]["quality"]["silver_rows"]:
            raise ValueError("Behavior event count differs from the Silver manifest")
        return {"behavior": summary}

    manifest, reused = materialize(storage, destination, identity, build)
    if fault:
        fault("after_behavior_gold")
    summary = storage.read_json(destination + "/behavior.json")
    if summary != manifest["metadata"]["behavior"]:
        raise ValueError("Behavior Gold differs from its manifest")
    behavior.publish(scope_id, summary, product_rows(spark, destination), {"analysis_id": analysis_id}, fault=fault)
    if fault:
        fault("before_behavior_pointer")
    # No source of truth points at partial products; this write publishes the snapshot.
    store.db.behavior_runs.update_one({"_id": analysis_id}, {"$set": {
        "status": "published", "snapshot_id": scope_id, "gold_path": destination, "published_at": utc_now()}})
    if fault:
        fault("after_behavior_pointer")
    return {"passed": True, "analysis_id": analysis_id, "snapshot_id": scope_id, "gold_path": destination,
            "identity": identity, "summary": summary, "manifest": manifest, "reused": reused,
            "minimum_block_replicas": 2, "duration_seconds": round(time.monotonic() - started, 3)}


def status(store, analysis_id, item_id=None):
    run = store.db.behavior_runs.find_one({"_id": analysis_id})
    if run is None or run.get("status") != "published":
        raise ValueError("Historical behavior analysis is not published")
    return {"analysis_id": analysis_id, "gold_path": run["gold_path"],
            **BehaviorStore(store.db).page(run["snapshot_id"], is_final=True, item_id=item_id)}
