"""Create a bounded, sorted replay without changing the historical Silver snapshot."""
import json
from pathlib import Path

from pyspark.sql import functions as F

from ecommerce_analytics.contracts import canonical_json, sha256_file
from ecommerce_analytics.snapshots import materialize, write_json
from ecommerce_behavior.contracts import reference
from .contracts import (BUSINESS_FIELDS, MAX_EVENTS, business_hash, iso, paths,
                        validate_request)
from .kafka_io import end_offsets
from .metrics import metrics
from .storage import Storage, read_config


def prepare(spark, request, local_output, store):
    validate_request(request)
    storage = Storage(spark)
    destination = paths(request["run_id"], request["base"])["prepared"]
    identity = {"kind": "replay-prepared", "request": request}
    output = Path(local_output)

    def build(staging):
        original = storage.read_json(request["silver_path"] + "/manifest.json")
        source_identity = original["identity"]
        if any(source_identity.get(key) != value for key, value in {
                "kind": "silver", "source": "retailrocket", "dataset_version": request["dataset_version"],
                "schema_version": 1, "transform_version": "events-v1"}.items()):
            raise ValueError("Replay source is not the expected historical Silver contract")
        storage.checked(request["silver_path"], source_identity)
        source = spark.read.parquet(request["silver_path"] + "/events")
        from ecommerce_analytics.ingestion import silver_schema
        expected_types = {field.name: field.dataType.simpleString() for field in silver_schema()}
        if {field.name: field.dataType.simpleString() for field in source.schema} != expected_types:
            raise ValueError("Historical Silver schema differs from events-v1")
        selected = source.where((F.col("event_date") >= F.to_date(F.lit(request["start"]))) &
                                (F.col("event_date") <= F.to_date(F.lit(request["end"]))) &
                                (F.col("event_time") >= F.to_timestamp(F.lit(request["start"]))) &
                                (F.col("event_time") < F.to_timestamp(F.lit(request["end"]))))
        count = selected.count()
        if not 0 < count <= MAX_EVENTS:
            raise ValueError(f"Replay must contain 1..{MAX_EVENTS} events; got {count}")
        with output.open("x", encoding="utf-8") as destination_file:
            for row in selected.orderBy("event_time", "event_id").select(*BUSINESS_FIELDS).toLocalIterator():
                event = row.asDict()
                event["event_time"] = iso(event["event_time"])
                destination_file.write(canonical_json(event) + "\n")
        # The reference uses the same typed fields but its own committed artifact.
        from pyspark.sql import types as T
        # Hash only the small selected data on executors; never collect source events.
        hash_event = F.udf(lambda *values: business_hash(dict(zip(BUSINESS_FIELDS, values))), T.StringType())
        expected = metrics(selected.withColumn("business_sha256", hash_event(
            *[F.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") if key == "event_time" else F.col(key)
              for key in BUSINESS_FIELDS])))
        config = {**request, "request": request, "paths": paths(request["run_id"], request["base"]),
                  "event_count": count, "replay_sha256": sha256_file(output), "expected": expected,
                  "initial_offsets": end_offsets(request["topic"])}
        with output.open(encoding="utf-8") as replay_source:
            config["expected_behavior"] = reference((json.loads(line) for line in replay_source), request["run_id"])["summary"]
        storage.copy_from_local(str(output), staging + "/events.jsonl")
        if storage.sha256(staging + "/events.jsonl") != config["replay_sha256"]:
            raise ValueError("Prepared replay copy checksum mismatch")
        write_json(storage, staging + "/config.json", config)
        return {"event_count": count, "replay_sha256": config["replay_sha256"]}

    manifest, reused = materialize(storage, destination, identity, build)
    config = read_config(storage, destination)
    if not output.exists():
        storage.copy_to_local(destination + "/events.jsonl", str(output))
    if sha256_file(output) != config["replay_sha256"]:
        raise ValueError("Local replay copy checksum mismatch")
    store.ensure_run(config)
    return {"config": config, "manifest": manifest, "reused": reused, "local_output": str(output)}
