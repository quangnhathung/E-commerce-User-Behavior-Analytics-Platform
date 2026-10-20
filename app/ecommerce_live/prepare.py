"""Materialize a shared historical seed and immutable varied child sources."""
from datetime import timedelta
import hashlib
import tempfile

from ecommerce_analytics.contracts import canonical_json
from ecommerce_analytics.snapshots import materialize, write_json
from ecommerce_behavior.contracts import reference as behavior_reference
from ecommerce_streaming.contracts import instant, iso
from ecommerce_streaming.kafka_io import end_offsets
from ecommerce_streaming.storage import read_config
from ecommerce_streaming_ml.contracts import paths
from ecommerce_streaming_ml.prepare import expected_metrics
from .contracts import child_request, events


def prepare_seed(storage, request):
    from pyspark.sql import functions as F
    from ecommerce_analytics.ingestion import silver_schema
    destination = request["base"] + "/live/" + request["live_id"] + "/seed"
    identity = {"kind": "live-seed", "request": request}

    def build(staging):
        source = storage.spark.read.schema(silver_schema()).parquet(request["silver_path"] + "/events")
        end = instant(request["start"])
        start = end - timedelta(days=7)
        seed = source.where((F.col("event_date") >= F.lit(start.date())) &
            (F.col("event_date") <= F.lit((end - timedelta(milliseconds=1)).date())) &
            (F.col("event_time") >= F.lit(start)) & (F.col("event_time") < F.lit(end)))
        seed.write.parquet(staging + "/events")
        return {"rows": seed.count(), "start": iso(start), "end": request["start"]}

    manifest, _ = materialize(storage, destination, identity, build)
    return {**manifest["metadata"], "path": destination + "/events", "root": destination, "identity": identity}


def prepare_segment(storage, store, request, index, bindings, seed):
    from pyspark.sql import functions as F
    child = child_request(request, index)
    destination = paths(child["run_id"], child["base"])["prepared"]
    identity = {"kind": "replay-prepared", "request": child}
    existing = store.db.stream_runs.find_one({"_id": child["run_id"]})
    if existing is not None and existing["config"].get("request") != child:
        raise ValueError("Live child RUN_ID belongs to another configuration")
    source = events(child)
    content = "".join(canonical_json(row) + "\n" for row in source).encode()
    checksum = hashlib.sha256(content).hexdigest()

    def build(staging):
        storage.checked(seed["root"], seed["identity"])
        who = sorted({row["visitor_id"] for row in source})
        if storage.spark.read.parquet(seed["path"]).where(F.col("visitor_id").isin(who)).limit(1).count():
            raise ValueError("Generated live visitors collide with the historical seed")
        config = {**child, "request": child, "paths": paths(child["run_id"], child["base"]), "bindings": bindings,
            "event_count": len(source), "expected": expected_metrics(source),
            "expected_behavior": behavior_reference(source, child["run_id"])["summary"],
            "initial_offsets": end_offsets(child["topic"]), "replay_sha256": checksum, "seed": seed}
        with tempfile.NamedTemporaryFile() as local:
            local.write(content); local.flush()
            storage.copy_from_local(local.name, staging + "/events.jsonl")
        if storage.sha256(staging + "/events.jsonl") != checksum:
            raise ValueError("Live source copy checksum mismatch")
        write_json(storage, staging + "/config.json", config)
        return {"event_count": len(source), "seed_rows": seed["rows"]}

    materialize(storage, destination, identity, build)
    config = read_config(storage, destination)
    if config["bindings"] != bindings or config["replay_sha256"] != checksum or config["seed"] != seed:
        raise ValueError("Live source/model/seed changed")
    store.ensure_run(config)
    return config, source
