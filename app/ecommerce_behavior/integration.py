"""Spark/HDFS/Mongo tests confined to one UUID namespace; no business data removed."""
from datetime import date, datetime, timedelta, timezone
import hashlib
import random
import unittest
import uuid

from pyspark.sql import types as T
from ecommerce_analytics.ingestion import silver_schema
from ecommerce_analytics.snapshots import materialize
from ecommerce_streaming.storage import Storage
from .contracts import BEHAVIOR_VERSION, instant, iso, reference
from .historical import analyze, status
from .pipeline import tables
from .store import BehaviorStore


def fixture_events():
    origin = datetime(2015, 9, 10, 23, 45, tzinfo=timezone.utc)
    values = [(0, "001", "10", "view"), (1, "001", "20", "view"),
              (2, "001", "20", "add_to_cart"), (3, "001", "10", "purchase"),
              (4, "001", "20", "purchase"), (1_804_000, "001", "10", "view"),
              (3_604_001, "001", "10", "purchase"),
              (0, "2", "30", "view"), (0, "2", "30", "add_to_cart"),
              (1, "2", "30", "purchase"), (0, "3", "40", "purchase")]
    return [{"event_id": hashlib.sha256(str(index).encode()).hexdigest(), "visitor_id": visitor,
             "item_id": item, "event_type": kind, "event_time": iso(origin + timedelta(milliseconds=milliseconds))}
            for index, (milliseconds, visitor, item, kind) in enumerate(values)]


def run_tests(spark, store, code_sha256):
    base = "/ecommerce/_tests/behavior/" + uuid.uuid4().hex
    source_root = base + "/silver"
    storage = Storage(spark)
    events = fixture_events()
    schema = T.StructType([T.StructField(key, T.StringType()) for key in
        ("event_id", "visitor_id", "item_id", "event_type")] + [T.StructField("event_time", T.TimestampType())])

    def frame(rows):
        return spark.createDataFrame([{**row, "event_time": instant(row["event_time"])} for row in rows], schema).repartition(4)

    def serialized(row):
        return {key: iso(value) if isinstance(value, datetime) else value.isoformat() if isinstance(value, date) else value
                for key, value in row.asDict().items()}

    def check_tables(test, rows):
        expected = reference(rows, "fixture")
        with tables(frame(rows), "fixture") as frames:
            for actual, name, keys in zip(frames, ("sessions", "session_items", "products"),
                    (("session_id",), ("session_id", "item_id"), ("item_id",))):
                values = [serialized(row) for row in actual.orderBy(*keys).toLocalIterator()]
                test.assertEqual(expected[name], values, name)

    def request(suffix):
        return {"behavior_version": BEHAVIOR_VERSION, "code_sha256": code_sha256,
                "dataset_version": "a" * 64, "silver_path": source_root, "base": base + "/" + suffix}

    def build_source(staging):
        rows = []
        # More than one Mongo batch, to exercise a genuinely partial product write.
        for index, row in enumerate(events + [{"event_id": hashlib.sha256(f"extra-{i}".encode()).hexdigest(),
                "visitor_id": str(5000 + i), "item_id": str(1000 + i), "event_type": "view",
                "event_time": events[0]["event_time"]} for i in range(110)]):
            timestamp = instant(row["event_time"])
            rows.append({**row, "schema_version": 1, "dataset_version": "a" * 64, "source": "retailrocket",
                "run_id": "history-fixture", "transaction_id": str(index) if row["event_type"] == "purchase" else None,
                "event_time": timestamp, "event_date": timestamp.date(), "emitted_at": None,
                "source_file": "events.csv", "source_row_number": index + 2})
        spark.createDataFrame(rows, silver_schema()).write.partitionBy("event_date").parquet(staging + "/events")
        return {"quality": {"silver_rows": len(rows)}}

    class BehaviorTests(unittest.TestCase):
        def test_spark_tables_match_reference_boundaries_and_chains(self):
            check_tables(self, list(reversed(events)))

        def test_empty_silver_has_zero_sessions_and_products(self):
            check_tables(self, [])

        def test_seeded_unsorted_events_match_reference(self):
            generator = random.Random(37)
            rows = [{"event_id": hashlib.sha256(f"seed-{index}".encode()).hexdigest(),
                     "visitor_id": str(generator.randrange(12)), "item_id": str(generator.randrange(8)),
                     "event_type": generator.choice(("view", "add_to_cart", "purchase")),
                     "event_time": iso(instant(events[0]["event_time"]) + timedelta(milliseconds=generator.randrange(9_000_000)))}
                    for index in range(300)]
            generator.shuffle(rows)
            check_tables(self, rows)

        def test_historical_publish_reuse_and_mongo_readback(self):
            first = analyze(spark, store, request("reuse"))
            second = analyze(spark, store, request("reuse"))
            self.assertFalse(first["reused"])
            self.assertTrue(second["reused"])
            self.assertEqual(first["manifest"], second["manifest"])
            visible = status(store, first["analysis_id"])
            self.assertTrue(visible["is_final"])
            self.assertEqual(first["summary"], visible["overview"])
            self.assertEqual(20, len(visible["products"]))
            self.assertEqual(["10"], [row["item_id"] for row in status(store, first["analysis_id"], "10")["products"]])

        def test_historical_faults_never_publish_partial_products_and_retry_reuses_gold(self):
            for stage in ("after_behavior_gold", "mongo_mid_funnel_products", "before_behavior_pointer", "after_behavior_pointer"):
                with self.subTest(stage=stage):
                    def fault(actual):
                        if actual == stage:
                            raise RuntimeError("injected " + stage)
                    spec = request(stage)
                    with self.assertRaisesRegex(RuntimeError, "injected"):
                        analyze(spark, store, spec, fault=fault)
                    run = store.db.behavior_runs.find_one({"identity.request.base": spec["base"]})
                    if stage == "after_behavior_pointer":
                        self.assertTrue(status(store, run["analysis_id"])["is_final"])
                    else:
                        with self.assertRaisesRegex(ValueError, "not published"):
                            status(store, run["analysis_id"])
                    result = analyze(spark, store, spec)
                    self.assertTrue(result["reused"])
                    BehaviorStore(store.db).verify(result["snapshot_id"], result["summary"])

        def test_missing_gold_marker_is_refused(self):
            spec = request("missing-marker")
            result = analyze(spark, store, spec)
            storage.remove(result["gold_path"] + "/_SUCCESS")
            with self.assertRaisesRegex(ValueError, "Incomplete snapshot"):
                analyze(spark, store, spec)

    result = None
    try:
        materialize(storage, source_root, {"kind": "silver", "source": "retailrocket", "dataset_version": "a" * 64,
                    "schema_version": 1, "transform_version": "events-v1"}, build_source)
        outcome = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(BehaviorTests))
        if not outcome.wasSuccessful():
            raise RuntimeError(f"Behavior integration failed: {len(outcome.failures)} failures, {len(outcome.errors)} errors")
        result = {"passed": True, "tests_run": outcome.testsRun, "test_root": base, "minimum_block_replicas": 2}
    finally:
        errors = []
        try:
            ids = [row["analysis_id"] for row in store.db.behavior_runs.find({"identity.request.silver_path": source_root})]
            for name in ("funnel_products", "funnel_overview"):
                store.db[name].delete_many({"analysis_id": {"$in": ids}})
            store.db.behavior_runs.delete_many({"analysis_id": {"$in": ids}})
        except Exception as error:
            errors.append(type(error).__name__)
        try:
            storage.remove(base)
        except Exception as error:
            errors.append(type(error).__name__)
        if errors:
            raise RuntimeError("Behavior test cleanup failed: " + ", ".join(errors))
    return {**result, "cleaned": True}
