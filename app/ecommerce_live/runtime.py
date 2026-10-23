"""One Spark application/model pair; bounded sequential runs and a producer thread."""
import json
import os
import shutil
import threading
import time

from ecommerce_streaming.contracts import drained, utc_now
from ecommerce_streaming.storage import Storage, read_config
from ecommerce_streaming_ml.batch import Processor
from ecommerce_streaming_ml.models import Models, resolve
from ecommerce_streaming_ml.stream import final_report, verify_committed
from .prepare import prepare_seed, prepare_segment
from .producer import emit


def run_segment(spark, storage, store, config, source, models, owner, stopping):
    started = time.monotonic()
    read_config(storage, config["paths"]["prepared"], config)
    verify_committed(storage, store, config)
    checkpoint = config["paths"]["checkpoint"]
    state = store.status(config)
    batches = any(storage.batch_paths(config["paths"][kind]) for kind in ("bronze", "silver", "gold"))
    if batches and not storage.exists(checkpoint + "/metadata"):
        raise ValueError("Live checkpoint missing; restore it before resuming")
    if state["consumer"]["status"] == "completed":
        if state["committed_batch_id"] not in storage.checkpoint_commits(checkpoint):
            raise ValueError("Completed live segment has no final checkpoint commit")
        report = final_report(storage, store, config, models, started=started)
        store.finish(config, report)
        return report
    store.consumer_status(config, "running")
    incoming = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
        .option("subscribe", config["topic"]).option("startingOffsets", json.dumps({config["topic"]: config["initial_offsets"]}))
        .option("failOnDataLoss", "true").option("maxOffsetsPerTrigger", 200).load())
    query = (incoming.writeStream.foreachBatch(Processor(spark, storage, store, config, models))
        .option("checkpointLocation", checkpoint).trigger(processingTime="15 seconds").start())
    errors, abort = [], threading.Event()

    def produce():
        try:
            emit(config, source, store, owner, stopping=lambda: abort.is_set() or stopping())
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=produce, daemon=True)
    try:
        store.consumer_status(config, "running", startup_seconds=round(time.monotonic() - started, 3))
        print("LIVE_SEGMENT_READY=" + config["run_id"], flush=True)
        thread.start()
        stop_started = None
        while query.isActive:
            query.awaitTermination(1)
            if errors:
                raise errors[0]
            state = store.status(config)
            batch = state["batch"]
            if stopping():
                stop_started = stop_started or time.monotonic()
                if time.monotonic() - stop_started > 180:
                    raise TimeoutError("Live pause could not drain its ACKs within 180 seconds")
            done = state["producer"]["status"] in ("completed", "paused") and not thread.is_alive()
            processed = batch["processed_offsets"] if batch else config["initial_offsets"]
            committed = not batch or state["committed_batch_id"] in storage.checkpoint_commits(checkpoint)
            if done and drained(processed, state["producer"]["end_offsets"]) and committed:
                query.stop(); query.awaitTermination(30)
                if state["producer"]["status"] == "paused":
                    store.consumer_status(config, "paused")
                    return None
                report = final_report(storage, store, config, models, started=started)
                store.finish(config, report)
                return report
        raise RuntimeError("Live query stopped before ACK reconciliation")
    except BaseException as error:
        store.consumer_status(config, "failed", error_type=type(error).__name__)
        raise
    finally:
        abort.set()
        if thread.ident is not None:
            thread.join(timeout=20)
        if query.isActive:
            query.stop()
        if thread.is_alive():
            raise RuntimeError("Live producer did not stop; retain its attempt guard")


def run(spark, store, request, hashes, owner):
    identity = request["live_id"]
    store.ensure_session(request, owner)
    halted, heartbeat_errors = threading.Event(), []

    def heartbeat():
        while not halted.is_set():
            try:
                store.state(identity, owner, heartbeat_at=utc_now())
            except BaseException as error:
                heartbeat_errors.append(error)
                return
            halted.wait(5)

    beat = threading.Thread(target=heartbeat, daemon=True)
    beat.start()

    def stopping():
        if heartbeat_errors:
            raise RuntimeError("Live heartbeat failed") from heartbeat_errors[0]
        return store.session(identity, owner)["stop_requested"]

    try:
        storage = Storage(spark)
        bindings = resolve(spark, storage, store.db, request, hashes)
        seed = prepare_seed(storage, request)
        models = Models(spark, bindings)
        while not stopping():
            session = store.session(identity, owner)
            index = session["completed_segments"] + 1
            if request["max_segments"] and index > request["max_segments"]:
                break
            minimum = request["minimum_disk_gib"] * 1024 ** 3
            if min(shutil.disk_usage("/tmp").free, shutil.disk_usage("/opt/app").free,
                   int(storage.fs.getStatus().getRemaining())) < minimum:
                raise RuntimeError("Live disk reserve reached; historical snapshots retained")
            store.state(identity, owner, status="preparing")
            config, source = prepare_segment(storage, store, request, index, bindings, seed)
            store.state(identity, owner, active_run_id=config["run_id"], segment_index=index, status="running")
            report = run_segment(spark, storage, store, config, source, models, owner, stopping)
            if report is None:
                break
            store.finish_segment(config, owner, report)
            print("LIVE_SEGMENT_RESULT=" + json.dumps(report, sort_keys=True), flush=True)
        session = store.session(identity, owner)
        status = "stopped" if session["stop_requested"] else "completed"
        store.state(identity, owner, status=status, heartbeat_at=utc_now())
        return {"passed": True, "live_id": identity, "status": status,
            "completed_segments": session["completed_segments"], "completed_events": session["completed_events"]}
    except BaseException:
        store.state(identity, owner, status="failed", heartbeat_at=utc_now())
        raise
    finally:
        halted.set(); beat.join(timeout=15)
