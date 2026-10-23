"""Pin models and materialize a pre-replay seed and bounded immutable source."""
import json
from datetime import timedelta
from pathlib import Path

from ecommerce_analytics.contracts import canonical_json, sha256_file
from ecommerce_analytics.snapshots import materialize, write_json
from ecommerce_behavior.contracts import reference as behavior_reference
from ecommerce_streaming.contracts import BUSINESS_FIELDS, MAX_EVENTS, business_hash, instant, iso, reference_metrics
from ecommerce_streaming.kafka_io import end_offsets
from ecommerce_streaming.storage import Storage, read_config
from .contracts import paths, simulator_events, validate_request
from .models import resolve


def expected_metrics(events):
    import hashlib
    result, checksum = reference_metrics(events), hashlib.sha256()
    for value in sorted(events, key=lambda value: value["event_id"]):
        checksum.update((canonical_json([value["event_id"], business_hash(value)]) + "\n").encode())
    return {**result, "events_sha256": checksum.hexdigest()}


def prepare(spark, request, local_output, store, hashes):
    from pyspark.sql import functions as F
    from ecommerce_analytics.ingestion import silver_schema
    validate_request(request)
    storage = Storage(spark)
    destination = paths(request["run_id"], request["base"])["prepared"]
    identity = {"kind": "replay-prepared", "request": request}
    # Check global RUN_ID ownership before doing any writes or expensive preparation.
    existing = store.db.stream_runs.find_one({"_id": request["run_id"]})
    if existing is not None and existing["config"].get("request") != request:
        raise ValueError("RUN_ID already belongs to another immutable configuration")
    bindings = resolve(spark, storage, store.db, request, hashes)
    output = Path(local_output)

    def build(staging):
        source = spark.read.schema(silver_schema()).parquet(request["silver_path"] + "/events")
        start, end = instant(request["start"]), instant(request["end"])
        seed_start = start - timedelta(days=7)
        seed = source.where((F.col("event_date") >= F.lit(seed_start.date())) &
            (F.col("event_date") <= F.lit((start - timedelta(milliseconds=1)).date())) &
            (F.col("event_time") >= F.lit(seed_start)) & (F.col("event_time") < F.lit(start)))
        if request["demo_source"] == "replay":
            selected = source.where((F.col("event_date") >= F.lit(start.date())) &
                (F.col("event_date") <= F.lit((end - timedelta(milliseconds=1)).date())) &
                (F.col("event_time") >= F.lit(start)) & (F.col("event_time") < F.lit(end)))
            count = selected.count()
            if not 0 < count <= MAX_EVENTS:
                raise ValueError(f"Replay must have 1..{MAX_EVENTS} events; got {count}")
            # This is the explicitly bounded replay, never the historical seed/full source.
            events = []
            for row in selected.select(*BUSINESS_FIELDS).orderBy("event_time", "event_id").toLocalIterator():
                value = row.asDict(); value["event_time"] = iso(value["event_time"]); events.append(value)
        else:
            events = simulator_events(request)
            if seed.where(F.col("visitor_id").isin([event["visitor_id"] for event in events])).limit(1).count():
                raise ValueError("Simulator visitors unexpectedly have historical seed data")
        seed.write.mode("errorifexists").parquet(staging + "/seed")
        seed_count = seed.count()
        with output.open("x", encoding="utf-8") as out:
            for value in events:
                out.write(canonical_json(value) + "\n")
        config = {**request, "request": request, "paths": paths(request["run_id"], request["base"]),
            "bindings": bindings, "event_count": len(events), "expected": expected_metrics(events),
            "expected_behavior": behavior_reference(events, request["run_id"])["summary"],
            "initial_offsets": end_offsets(request["topic"]), "replay_sha256": sha256_file(output),
            "seed": {"start": iso(seed_start), "end": request["start"], "rows": seed_count,
                     "path": destination + "/seed"}}
        storage.copy_from_local(str(output), staging + "/events.jsonl")
        if storage.sha256(staging + "/events.jsonl") != config["replay_sha256"]:
            raise ValueError("Streaming ML source copy checksum mismatch")
        write_json(storage, staging + "/config.json", config)
        return {"event_count": len(events), "seed_rows": seed_count}

    manifest, reused = materialize(storage, destination, identity, build)
    config = read_config(storage, destination)
    if config["bindings"] != bindings:
        raise ValueError("Pinned model/policy bindings changed; restore them or use a new run")
    if not output.exists():
        storage.copy_to_local(destination + "/events.jsonl", str(output))
    if sha256_file(output) != config["replay_sha256"]:
        raise ValueError("Local streaming ML source checksum mismatch")
    store.ensure_run(config)
    return {"passed": True, "run_id": request["run_id"], "config": config, "manifest": manifest,
            "reused": reused, "minimum_block_replicas": 2}
