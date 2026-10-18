"""Finite streaming lifecycle: checkpoint recovery, offset drain and final reconciliation."""
import json
import time

from .batch import Processor
from ecommerce_streaming.contracts import drained
from ecommerce_streaming.storage import Storage, read_config
from ecommerce_streaming.stream import verify_committed as verify_base, final_report as base_final_report
from ecommerce_streaming.batch import read_events
from ecommerce_prediction.data import KNOWN_SOURCE
from .inference import reconcile
from .models import Models, resolve


def verify_committed(storage, store, config):
    verify_base(storage, config)
    current = store.load(config)["committed_batch_id"]
    for row in store.db.stream_batches.find({"run_id": config["run_id"]}):
        if row["batch_id"] > current:
            continue  # A prepared future batch is allowed before the atomic pointer.
        suffix = f"/{row['batch_id']:020d}"
        roots = {kind: config["paths"][kind] + suffix for kind in ("bronze", "silver", "gold")}
        manifests = {kind: storage.checked(root, storage.read_json(root + "/manifest.json")["identity"])
                     for kind, root in roots.items()}
        if (manifests["silver"]["identity"]["bronze_sha256"] != storage.sha256(roots["bronze"] + "/manifest.json") or
                manifests["gold"]["identity"]["silver_sha256"] != storage.sha256(roots["silver"] + "/manifest.json")):
            raise ValueError("Committed streaming ML Bronze/Silver/Gold manifest links changed")


def final_report(storage, store, config, models, *, started):
    report = base_final_report(storage, store, config, started=started)
    verify_committed(storage, store, config)
    state = store.status(config)
    batch_id = state["committed_batch_id"]
    accepted = read_events(storage.spark, storage.batch_paths(config["paths"]["silver"], through=batch_id))
    ml = reconcile(storage.spark, storage, config, accepted, state["batch"]["gold_path"], models)
    store.verify_ml(config, batch_id, ml)
    if (config["demo_source"] == "replay" and config["dataset_version"] == KNOWN_SOURCE and
            config["start"] == "2015-09-18T00:00:00.000Z" and config["end"] == "2015-09-18T03:00:00.000Z"):
        if report["overview"]["processed_events"] != 1528 or ml["fingerprint"]["rows"] != 1016:
            raise ValueError("Default ML replay differs from the Retailrocket baseline")
    return {**report, "scoring": ml, "bindings": config["bindings"], "demo_source": config["demo_source"],
            "batch_reconciled": True, "startup_seconds": state["consumer"].get("startup_seconds"),
            "first_batch_seconds": report["batch_seconds"][0] if report["batch_seconds"] else None}


def run_stream(spark, config, store, hashes, fault=None):
    try:
        return _run_stream(spark, config, store, hashes, fault)
    except BaseException as error:
        try:
            store.consumer_status(config, "interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
                                  error_type=type(error).__name__, reason=str(error))
        except Exception:
            pass
        raise


def _run_stream(spark, config, store, hashes, fault=None):
    started = time.monotonic()
    storage = Storage(spark)
    read_config(storage, config["paths"]["prepared"], config)
    if resolve(spark, storage, store.db, config["request"], hashes) != config["bindings"]:
        raise ValueError("Pinned streaming ML model/policy bindings changed")
    models = Models(spark, config["bindings"])
    verify_committed(storage, store, config)
    prior = storage.batch_paths(config["paths"]["silver"])
    checkpoint = config["paths"]["checkpoint"]
    any_batches = prior or storage.batch_paths(config["paths"]["bronze"]) or storage.batch_paths(config["paths"]["gold"])
    if any_batches and not storage.exists(checkpoint + "/metadata"):
        raise ValueError("Checkpoint missing while committed batches exist; use a new run or restore checkpoint")
    commits = storage.checkpoint_commits(checkpoint)
    if prior:
        document = store.load(config)
        maximum = int(prior[-1].rsplit("/", 1)[1])
        if document["committed_batch_id"] > maximum:
            raise ValueError("Run registry points beyond committed HDFS batches")
        for root in prior:
            number = int(root.rsplit("/", 1)[1])
            if commits and number <= max(commits) and store.db.stream_batches.find_one(
                    {"_id": f"{config['run_id']}:{number}"}) is None:
                raise ValueError("Checkpoint is ahead of the published MongoDB batch history")
    state = store.status(config)
    if state["consumer"]["status"] == "completed":
        if state["committed_batch_id"] not in commits:
            raise ValueError("Completed run has no matching final checkpoint commit")
        report = {**final_report(storage, store, config, models, started=started), "reused": True}
        store.finish(config, report)
        return report
    store.consumer_status(config, "running")
    query = None
    try:
        source = (spark.readStream.format("kafka")
                  .option("kafka.bootstrap.servers", __import__("os").environ["KAFKA_BOOTSTRAP_SERVERS"])
                  .option("subscribe", config["topic"])
                  .option("startingOffsets", json.dumps({config["topic"]: config["initial_offsets"]}))
                  .option("failOnDataLoss", "true").option("maxOffsetsPerTrigger", 10000).load())
        query = (source.writeStream.foreachBatch(Processor(spark, storage, store, config, models, fault=fault))
                 .option("checkpointLocation", checkpoint).trigger(processingTime="15 seconds").start())
        store.consumer_status(config, "running", startup_seconds=round(time.monotonic() - started, 3))
        print("STREAM_ML_READY=" + config["run_id"], flush=True)
        while query.isActive:
            query.awaitTermination(2)
            state = store.status(config)
            batch = state["batch"]
            if (state["producer"]["status"] == "completed" and batch is not None and
                    drained(batch["processed_offsets"], state["producer"]["end_offsets"]) and
                    storage.exists(checkpoint + "/commits/" + str(state["committed_batch_id"]))):
                query.stop()
                query.awaitTermination(30)
                report = final_report(storage, store, config, models, started=started)
                store.finish(config, report)
                return report
        if query.exception() is not None:
            raise RuntimeError(str(query.exception()))
        raise RuntimeError("Streaming stopped before its acknowledged replay drained")
    except BaseException as error:
        try:
            store.consumer_status(config, "interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
                                  error_type=type(error).__name__)
        except Exception:
            pass  # Preserve the original failure if Mongo is unavailable.
        raise
    finally:
        if query is not None and query.isActive:
            query.stop()
