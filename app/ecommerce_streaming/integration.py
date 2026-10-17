"""Real Spark/HDFS/Kafka/Mongo assertions in disposable test namespaces."""
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import tempfile
import time
import unittest
import uuid

from pyspark.sql import types as T
from ecommerce_analytics.contracts import canonical_json
from ecommerce_analytics.snapshots import materialize, write_json
from .batch import Processor
from .contracts import (BUSINESS_FIELDS, PIPELINE_VERSION, business_hash, paths,
                        reference_metrics, utc_now, instant)
from .kafka_io import (create_test_topics, delete_test_topics, end_offsets,
                       replay, broker_config)
from .storage import Storage
from .stream import run_stream


def fixture_events():
    events = []
    for index, (minute, visitor, item, event, transaction) in enumerate([
            (0, "1", "10", "view", None), (1, "2", "20", "view", None),
            (2, "1", "10", "add_to_cart", None), (6, "1", "10", "purchase", "900"),
            (6, "1", "11", "purchase", "900"), (7, "3", "30", "view", None)]):
        events.append({"schema_version": 1, "dataset_version": "a" * 64,
                       "event_id": hashlib.sha256(str(index).encode()).hexdigest(), "source": "retailrocket",
                       "visitor_id": visitor, "item_id": item, "event_type": event, "transaction_id": transaction,
                       "event_time": f"2015-09-10T10:{minute:02d}:00.000Z"})
    return events


def expected(events):
    result = reference_metrics(events)
    checksum = hashlib.sha256()
    for event in sorted(events, key=lambda value: value["event_id"]):
        checksum.update((canonical_json([event["event_id"], business_hash(event)]) + "\n").encode())
    return {**result, "events_sha256": checksum.hexdigest()}


def run_tests(spark, store, code_sha256):
    token = uuid.uuid4().hex
    raw, dlq = "app-stream-test-" + token, "app-stream-dlq-" + token
    base = "/ecommerce/_tests/stream/" + token
    storage = Storage(spark)
    run_ids, topics = [], []
    results = None
    events = fixture_events()

    def config_for(source=events, reference=None):
        run_id = "test-" + uuid.uuid4().hex
        run_ids.append(run_id)
        request = {"run_id": run_id, "start": "2015-09-10T10:00:00.000Z", "end": "2015-09-10T16:00:00.000Z",
                   "rate": 10, "pipeline_version": PIPELINE_VERSION, "code_sha256": code_sha256,
                   "dataset_version": "a" * 64, "silver_path": "fixture", "base": base,
                   "topic": raw, "dlq_topic": dlq}
        content = "".join(canonical_json(event) + "\n" for event in source).encode()
        config = {**request, "request": request, "paths": paths(run_id, base), "event_count": len(source),
                  "replay_sha256": hashlib.sha256(content).hexdigest(), "expected": expected(reference if reference is not None else source),
                  "initial_offsets": end_offsets(raw)}

        def build(staging):
            storage.write_bytes(staging + "/events.jsonl", content)
            write_json(storage, staging + "/config.json", config)
            return {"event_count": len(source)}

        materialize(storage, config["paths"]["prepared"], {"kind": "replay-prepared", "request": request}, build)
        store.ensure_run(config)
        return config

    def frame(config, payloads, start=None):
        offsets = dict(start or config["initial_offsets"])
        rows = []
        for index, payload in enumerate(payloads):
            partition = str(index % 3)
            value = payload if isinstance(payload, bytes) else canonical_json({
                **payload, "run_id": payload.get("run_id", config["run_id"]), "emitted_at": utc_now()}).encode()
            rows.append((b"fixture", value, raw, int(partition), offsets[partition], datetime.now(timezone.utc)))
            offsets[partition] += 1
        schema = T.StructType([T.StructField("key", T.BinaryType()), T.StructField("value", T.BinaryType()),
            T.StructField("topic", T.StringType()), T.StructField("partition", T.IntegerType()),
            T.StructField("offset", T.LongType()), T.StructField("timestamp", T.TimestampType())])
        return spark.createDataFrame(rows, schema).repartition(6), offsets

    def assert_kpi(test, config):
        state = store.status(config)
        test.assertEqual(config["expected"]["overview"], state["overview"])
        actual = storage.read_json(state["batch"]["gold_path"] + "/metrics.json")
        test.assertEqual(config["expected"], actual)

    try:
        # Record ownership before the asynchronous create: partial creation is cleaned too.
        topics = [raw, dlq]
        create_test_topics(raw, dlq)

        class StreamTests(unittest.TestCase):
            def test_prepare_sorted_snapshot_and_reuse(self):
                from ecommerce_analytics.ingestion import silver_schema
                from .prepare import prepare
                source_root = base + "/historical-fixture"
                rows = []
                for index, event in enumerate(reversed(events)):
                    timestamp = instant(event["event_time"])
                    rows.append({**event, "event_time": timestamp, "event_date": timestamp.date(),
                                 "run_id": "history-fixture", "emitted_at": None,
                                 "source_file": "events.csv", "source_row_number": index + 2})

                def build(staging):
                    spark.createDataFrame(rows, silver_schema()).write.partitionBy("event_date").parquet(staging + "/events")
                    return {"quality": {"silver_rows": len(rows)}}

                materialize(storage, source_root, {"kind": "silver", "source": "retailrocket",
                    "dataset_version": "a" * 64, "schema_version": 1, "transform_version": "events-v1"}, build)
                template = config_for()["request"]
                run_id = "test-" + uuid.uuid4().hex
                run_ids.append(run_id)
                request = {**template, "run_id": run_id, "silver_path": source_root}
                with tempfile.TemporaryDirectory(prefix="stream-prepare-test-") as local:
                    first = prepare(spark, request, str(Path(local) / "first.jsonl"), store)
                    second = prepare(spark, request, str(Path(local) / "second.jsonl"), store)
                    self.assertFalse(first["reused"])
                    self.assertTrue(second["reused"])
                    self.assertEqual(first["manifest"], second["manifest"])
                    self.assertEqual(expected(events), first["config"]["expected"])
                    prepared = [json.loads(line) for line in (Path(local) / "first.jsonl").read_text().splitlines()]
                    self.assertEqual(sorted(events, key=lambda event: (event["event_time"], event["event_id"])), prepared)

            def test_exact_kpi_dedup_and_validation(self):
                config = config_for()
                first, offsets = frame(config, events[:3])
                process = Processor(spark, storage, store, config)
                process(first, 0)
                conflict = {**events[0], "item_id": "999"}
                second, _ = frame(config, events[3:] + [events[0], b"{", conflict,
                                                        {**events[0], "run_id": "other-run"}], offsets)
                process(second, 1)
                assert_kpi(self, config)
                counts = store.status(config)["batch"]["counts"]
                self.assertEqual({"accepted": 3, "duplicate": 1, "invalid": 2, "late": 0, "foreign": 1}, counts)
                self.assertEqual(1, store.status(config)["overview"]["transactions"])
                self.assertEqual(2, store.status(config)["overview"]["active_visitors"])

            def test_lateness_boundary(self):
                high = {**events[0], "event_time": "2015-09-10T10:20:00.000Z"}
                boundary = {**events[1], "event_time": "2015-09-10T10:10:00.000Z"}
                late = {**events[2], "event_time": "2015-09-10T10:09:59.999Z"}
                config = config_for([high, boundary, late], [high, boundary])
                process = Processor(spark, storage, store, config)
                first, offsets = frame(config, [high])
                process(first, 0)
                second, _ = frame(config, [boundary, late], offsets)
                process(second, 1)
                self.assertEqual(1, store.status(config)["batch"]["counts"]["late"])
                assert_kpi(self, config)

            def test_fault_retries_preserve_snapshot_and_counts(self):
                for stage in ("after_bronze", "after_silver", "after_gold", "after_dlq",
                              "mongo_mid_minutes", "mongo_before_pointer", "after_mongo_publish"):
                    with self.subTest(stage=stage):
                        config = config_for()
                        first, offsets = frame(config, events[:3])

                        def fault(actual):
                            if actual == stage:
                                raise RuntimeError("injected " + stage)

                        with self.assertRaisesRegex(RuntimeError, "injected"):
                            Processor(spark, storage, store, config, fault)(first, 0)
                        root = config["paths"]["silver"] + "/00000000000000000000"
                        before = storage.sha256(root + "/manifest.json") if storage.exists(root) else None
                        Processor(spark, storage, store, config)(first, 0)
                        if before:
                            self.assertEqual(before, storage.sha256(root + "/manifest.json"))
                        second, _ = frame(config, events[3:], offsets)
                        Processor(spark, storage, store, config)(second, 1)
                        assert_kpi(self, config)
                        self.assertEqual(2, store.db.stream_batches.count_documents({"run_id": config["run_id"]}))

            def test_missing_checkpoint_is_refused(self):
                config = config_for()
                incoming, _ = frame(config, events)
                Processor(spark, storage, store, config)(incoming, 0)
                with self.assertRaisesRegex(ValueError, "Checkpoint missing"):
                    run_stream(spark, config, store)

            def test_missing_completion_marker_is_refused(self):
                config = config_for()
                incoming, _ = frame(config, events)
                process = Processor(spark, storage, store, config)
                process(incoming, 0)
                root = config["paths"]["silver"] + "/00000000000000000000"
                storage.remove(root + "/_SUCCESS")
                with self.assertRaisesRegex(ValueError, "Incomplete snapshot"):
                    process(incoming, 0)

            def test_dlq_retry_has_stable_source_identity(self):
                from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
                config = config_for()
                incoming, _ = frame(config, [events[0], b"{"])

                def fault(stage):
                    if stage == "after_dlq":
                        raise RuntimeError("injected after DLQ ACK")

                with self.assertRaisesRegex(RuntimeError, "injected"):
                    Processor(spark, storage, store, config, fault)(incoming, 0)
                Processor(spark, storage, store, config)(incoming, 0)
                consumer = Consumer({**broker_config(), "group.id": "verify-" + uuid.uuid4().hex,
                                     "enable.auto.commit": False})
                records = []
                try:
                    consumer.assign([TopicPartition(dlq, 0, OFFSET_BEGINNING)])
                    deadline = time.monotonic() + 20
                    while len(records) < 2 and time.monotonic() < deadline:
                        message = consumer.poll(1)
                        if message is None:
                            continue
                        if message.error():
                            raise RuntimeError(str(message.error()))
                        value = json.loads(message.value())
                        if value["run_id"] == config["run_id"]:
                            records.append(value)
                finally:
                    consumer.close()
                self.assertEqual(2, len(records))
                self.assertEqual(1, len({row["dlq_id"] for row in records}))
                self.assertTrue(all(row["error_codes"] == ["MALFORMED_JSON"] for row in records))

            def test_real_kafka_checkpoint_retry_and_auto_stop(self):
                config = config_for()
                replay(io.StringIO("".join(canonical_json(event) + "\n" for event in events)), config, store)

                def fault(stage):
                    if stage == "after_mongo_publish":
                        raise RuntimeError("injected before Spark checkpoint commit")

                with self.assertRaisesRegex(Exception, "injected before Spark checkpoint commit"):
                    run_stream(spark, config, store, fault=fault)
                self.assertTrue(storage.exists(config["paths"]["checkpoint"] + "/metadata"))
                report = run_stream(spark, config, store)
                self.assertTrue(report["passed"])
                self.assertEqual(0, report["backlog"])
                self.assertEqual({"spark-worker-1", "spark-worker-2"}, set(report["worker_hosts"]))
                assert_kpi(self, config)

        outcome = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(StreamTests))
        if not outcome.wasSuccessful():
            raise RuntimeError(f"Streaming integration failed: {len(outcome.failures)} failures, {len(outcome.errors)} errors")
        results = {"passed": True, "tests_run": outcome.testsRun, "fault_scenarios": 7, "test_root": base,
                   "run_ids": run_ids, "minimum_block_replicas": 2}
    finally:
        errors = []
        for cleanup in (lambda: storage.remove(base), lambda: store.cleanup(run_ids), lambda: delete_test_topics(topics) if topics else None):
            try:
                cleanup()
            except Exception as error:
                errors.append(type(error).__name__)
        if errors:
            raise RuntimeError("Streaming test cleanup failed: " + ", ".join(errors))
    return {**results, "cleaned": True}
