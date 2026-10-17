"""Finite streaming lifecycle: checkpoint recovery, offset drain and final reconciliation."""
import json
import math
import time

from ecommerce_behavior.contracts import product_fingerprint
from ecommerce_behavior.pipeline import product_rows
from ecommerce_behavior.store import BehaviorStore
from .batch import Processor
from .contracts import digest, drained, instant, utc_now
from .storage import Storage, read_config


def verify_committed(storage, config):
    for kind in ("bronze", "silver", "gold"):
        for root in storage.batch_paths(config["paths"][kind]):
            manifest = storage.read_json(root + "/manifest.json")
            identity = manifest["identity"]
            if (identity.get("kind") != "stream-" + kind or
                    identity.get("run_config_sha256") != digest(config) or
                    identity.get("batch_id") != int(root.rsplit("/", 1)[1])):
                raise ValueError("Committed batch identity differs from the run")
            storage.checked(root, identity)


def final_report(storage, store, config, *, started):
    state = store.status(config)
    batch = state["batch"]
    if batch is None:
        raise ValueError("No committed streaming batch")
    actual = storage.read_json(batch["gold_path"] + "/metrics.json")
    if actual != config["expected"]:
        raise ValueError("Final event identities/business content or KPI differ from the prepared reference")
    if state["overview"] != actual["overview"]:
        raise ValueError("Published MongoDB overview differs from committed HDFS Gold")
    minutes = list(store.db.kpi_minute.find({"run_id": config["run_id"], "batch_id": state["committed_batch_id"]},
                                         {"_id": 0, "run_id": 0, "batch_id": 0}).sort("window_start", 1))
    if minutes != actual["minutes"]:
        raise ValueError("Published MongoDB minute KPI differs from committed HDFS Gold")
    behavior = storage.read_json(batch["gold_path"] + "/behavior.json")
    if behavior != config["expected_behavior"]:
        raise ValueError("Final session/funnel results differ from the independent prepared reference")
    fingerprint = product_fingerprint(product_rows(storage.spark, batch["gold_path"]))
    if fingerprint != {key: behavior[key] for key in fingerprint}:
        raise ValueError("Gold funnel products differ from the prepared reference")
    BehaviorStore(store.db).verify(f"{config['run_id']}:{state['committed_batch_id']}", behavior)
    if not drained(batch["processed_offsets"], state["producer"]["end_offsets"]):
        raise ValueError("The acknowledged producer offsets have not drained")
    workers = state["worker_hosts"]
    if config.get("require_two_workers", True) and set(workers) != {"spark-worker-1", "spark-worker-2"}:
        raise ValueError(f"Parsing did not execute on both workers: {workers}")
    verify_committed(storage, config)
    totals = {key: 0 for key in ("accepted", "duplicate", "invalid", "late", "foreign")}
    samples, durations = [], []
    rows = list(store.db.stream_batches.find({"run_id": config["run_id"]}).sort("batch_id", 1))
    for index, row in enumerate(rows):
        for key in totals:
            totals[key] += row["counts"][key]
        durations.append(row["batch_seconds"])
        if index > 0:  # Startup is reported separately from steady-state latency.
            samples.extend(row["latency_seconds"])
    samples.sort()
    p95 = samples[max(0, math.ceil(len(samples) * .95) - 1)] if samples else None
    producer = state["producer"]
    replay_seconds = ((instant(producer["completed_at"]) - instant(producer["started_at"])).total_seconds()
                      if producer.get("completed_at") and producer.get("started_at") else None)
    return {"run_id": config["run_id"], "passed": True, "completed_at": utc_now(),
            "duration_seconds": round(time.monotonic() - started, 3), "counts": totals,
            "replay_wall_seconds": replay_seconds,
            "acknowledged_rate_eps": config["event_count"] / replay_seconds if replay_seconds and replay_seconds > 0 else None,
            "overview": actual["overview"], "events_sha256": actual["events_sha256"],
            "worker_hosts": workers, "processed_offsets": batch["processed_offsets"],
            "target_offsets": state["producer"]["end_offsets"], "backlog": 0,
            "minimum_block_replicas": 2, "checkpoint_batch_id": state["committed_batch_id"],
            "steady_state_latency_p95_seconds": p95, "latency_samples": len(samples),
            "performance_target_met": p95 <= 60 if p95 is not None else None,
            "batch_seconds": durations, "behavior": behavior}


def run_stream(spark, config, store, fault=None):
    started = time.monotonic()
    storage = Storage(spark)
    read_config(storage, config["paths"]["prepared"], config)
    verify_committed(storage, config)
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
        report = {**final_report(storage, store, config, started=started), "reused": True}
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
        query = (source.writeStream.foreachBatch(Processor(spark, storage, store, config, fault=fault))
                 .option("checkpointLocation", checkpoint).trigger(processingTime="15 seconds").start())
        print("STREAM_READY=" + config["run_id"], flush=True)
        while query.isActive:
            query.awaitTermination(2)
            state = store.status(config)
            batch = state["batch"]
            if (state["producer"]["status"] == "completed" and batch is not None and
                    drained(batch["processed_offsets"], state["producer"]["end_offsets"]) and
                    storage.exists(checkpoint + "/commits/" + str(state["committed_batch_id"]))):
                query.stop()
                query.awaitTermination(30)
                report = final_report(storage, store, config, started=started)
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
