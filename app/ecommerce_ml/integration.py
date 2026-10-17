"""Live Spark/HDFS/Mongo acceptance, scoped exclusively to UUID-owned test data."""
from datetime import timedelta
import hashlib
import unittest
from unittest import mock
import uuid

from ecommerce_analytics.hdfs import Hdfs
from ecommerce_analytics.ingestion import silver_schema
from ecommerce_analytics.snapshots import materialize
from .contracts import DEFAULT_AS_OF, FEATURE_VERSION, FEATURES, choose, fingerprint, instant, iso, reference, window
from .features import build_features, feature_frame, load_features, rows
from .training import preprocessor, train_segments


def fixture_events():
    start, end = window(DEFAULT_AS_OF)
    result = []
    def add(visitor, item, kind, timestamp, transaction=None):
        result.append({"visitor_id": visitor, "item_id": item, "event_type": kind,
                       "event_time": iso(timestamp), "transaction_id": transaction})
    for index in range(120):
        visitor, group = f"{index:04d}", index % 3
        timestamp = start + timedelta(hours=2 + index % 48, milliseconds=index)
        for offset in range(1 + group * 4 + index % 2):
            add(visitor, str(offset % 3), "view", timestamp + timedelta(milliseconds=offset))
        for offset in range(group):
            add(visitor, str(offset), "add_to_cart", timestamp + timedelta(hours=24, milliseconds=offset))
        if group:
            for item in ("0", "1"):
                add(visitor, item, "purchase", timestamp + timedelta(hours=25), str(index))
    add("001", "01", "add_to_cart", start)
    add("001", "02", "purchase", end - timedelta(milliseconds=1), "0001")
    add("001", "03", "purchase", end - timedelta(milliseconds=1), "0001")
    add("99998", "01", "view", start - timedelta(milliseconds=1))
    add("99999", "01", "view", end)
    add("99999", "01", "view", end + timedelta(milliseconds=1))
    return result


def run_tests(spark, store, code_sha256):
    base = "/ecommerce/_tests/ml/" + uuid.uuid4().hex
    print("ML_TEST_ROOT=" + base, flush=True)
    storage, source_root = Hdfs(spark), base + "/silver"
    events, cache = fixture_events(), {}
    owned_models = set()
    source_identity = {"kind": "silver", "source": "retailrocket", "dataset_version": "a" * 64,
                       "schema_version": 1, "transform_version": "events-v1"}
    spec = {"feature_version": FEATURE_VERSION, "as_of": DEFAULT_AS_OF, "dataset_version": "a" * 64,
            "silver_path": source_root, "code_sha256": code_sha256, "base": base}

    def source_build(staging):
        values = []
        for index, event in enumerate(events):
            timestamp = instant(event["event_time"])
            values.append({**event, "event_time": timestamp, "event_date": timestamp.date(), "schema_version": 1,
                "dataset_version": "a" * 64, "source": "retailrocket", "run_id": "ml-fixture",
                "event_id": hashlib.sha256(str(index).encode()).hexdigest(), "emitted_at": None,
                "source_file": "events.csv", "source_row_number": index + 2})
        spark.createDataFrame(values, silver_schema()).repartition(4).write.partitionBy("event_date").parquet(staging + "/events")
        return {"quality": {"silver_rows": len(values),
            "min_event_time": min(row["event_time"] for row in events),
            "max_event_time": max(row["event_time"] for row in events)}}

    def features():
        if "features" not in cache:
            cache["features"] = build_features(spark, spec)
        return cache["features"]

    def model_spec():
        value = features()
        return {"feature_snapshot_id": value["feature_snapshot_id"], "feature_path": value["feature_path"],
            "feature_manifest_sha256": value["manifest_sha256"], "code_sha256": code_sha256, "base": base}

    def train(fault=None):
        # Registry is written before Gold. Remember ownership even when the operation raises.
        try:
            return train_segments(spark, store, model_spec(), fault=fault)
        finally:
            owned_models.update(row["model_version"] for row in store.db.segment_models.find({
                "identity.feature_path": features()["feature_path"]}))

    def fail_at(expected):
        def fault(actual):
            if actual == expected:
                raise RuntimeError("injected " + actual)
        return fault

    def clear_mongo():
        for name in ("visitor_segments", "segment_profiles", "segment_models"):
            store.db[name].delete_many({"model_version": {"$in": list(owned_models)}})

    class MLTests(unittest.TestCase):
        def test_a_features_match_oracle_and_reuse_without_rebuilding(self):
            expected = reference(list(reversed(events)))
            first = features()
            frame, _ = load_features(spark, storage, first["feature_path"], snapshot_id=first["feature_snapshot_id"])
            self.assertEqual(fingerprint(expected), fingerprint(rows(frame)))
            self.assertEqual(121, first["quality"]["fingerprint"]["rows"])
            with mock.patch(__package__ + ".features.feature_frame", side_effect=AssertionError("rebuilt")):
                second = build_features(spark, spec)
            self.assertTrue(second["reused"])
            self.assertEqual(first["manifest"], second["manifest"])

        def test_b_feature_publish_faults_and_invalid_version(self):
            with self.assertRaisesRegex(ValueError, "Unsupported feature"):
                build_features(spark, {**spec, "feature_version": "future"})
            for stage in ("before_feature_gold", "after_feature_gold"):
                request = {**spec, "base": base + "/" + stage}
                with self.assertRaisesRegex(RuntimeError, "injected"):
                    build_features(spark, request, fault=fail_at(stage))
                result = build_features(spark, request)
                self.assertEqual(stage == "after_feature_gold", result["reused"])

        def test_c_model_fault_before_gold_reload_and_no_refit_on_reuse(self):
            with self.assertRaisesRegex(RuntimeError, "injected"):
                train(fail_at("before_model_gold"))
            first = train()
            cache["model"] = first
            self.assertTrue(first["quality"]["reload_verified"])
            self.assertEqual(121, sum(value["count"] for value in first["quality"]["profiles"]))
            self.assertEqual(choose(first["quality"]["candidates"])["k"], first["quality"]["selected_k"])
            with mock.patch(__package__ + ".training.fit", side_effect=AssertionError("refit")):
                second = train()
            self.assertTrue(second["reused"])
            self.assertEqual(first["manifest"], second["manifest"])
            version = first["model_version"]
            self.assertEqual("001", store.status(version, "001")["visitor"]["visitor_id"])
            self.assertIsNone(store.status(version, "99999")["visitor"])

        def test_d_publication_faults_hide_partial_documents_and_retry(self):
            version = cache["model"]["model_version"]
            for stage in ("after_model_gold", "mongo_mid_assignments", "before_model_pointer", "after_model_pointer"):
                with self.subTest(stage=stage):
                    clear_mongo()
                    with self.assertRaisesRegex(RuntimeError, "injected"):
                        train(fail_at(stage))
                    if stage == "after_model_pointer":
                        self.assertEqual("published", store.status(version)["status"])
                    else:
                        with self.assertRaisesRegex(ValueError, "not published"):
                            store.status(version)
                    self.assertTrue(train()["reused"])
                    store.verify(version, cache["model"]["quality"])

        def test_e_constant_scaling_and_all_identical_candidates(self):
            from .native import NativeML
            from .training import fit
            frame, _ = load_features(spark, storage, features()["feature_path"])
            # A single real feature row repeated with distinct visitor IDs has zero variance.
            from pyspark.sql import functions as F
            single = frame.limit(1).drop("visitor_id").crossJoin(spark.range(6).select(F.col("id").cast("string").alias("visitor_id")))
            native = NativeML(spark)
            scaled = native.transform(preprocessor(spark).fit(single._jdf), single)
            for row in scaled.select(native.vector_array(scaled)).collect():
                self.assertEqual([0.0] * 8, row[0])
            with self.assertRaisesRegex(ValueError, "No valid"):
                fit(spark, single, storage, base + "/identical")

        def test_f_feature_schema_marker_and_model_corruption_refused(self):
            path = features()["feature_path"]
            with self.assertRaisesRegex(ValueError, "Snapshot identity"):
                from .features import checked
                checked(storage, path, {"kind": "features", "feature_version": "future"})
            # The schema validator must reject a typed column changed to text.
            from .features import validate_frame
            from pyspark.sql import functions as F
            frame, _ = load_features(spark, storage, path)
            with self.assertRaisesRegex(ValueError, "Feature schema"):
                validate_frame(frame.withColumn(FEATURES[0], F.col(FEATURES[0]).cast("string")), DEFAULT_AS_OF)
            with self.assertRaisesRegex(ValueError, "window is empty"):
                validate_frame(frame.limit(0), DEFAULT_AS_OF)
            model_path = cache["model"]["model_path"]
            storage.write_bytes(model_path + "/pipeline/unexpected-corruption", b"bad")
            with self.assertRaisesRegex(ValueError, "inventory mismatch"):
                train()
            storage.remove(model_path + "/pipeline/unexpected-corruption")
            storage.remove(path + "/_SUCCESS")
            with self.assertRaisesRegex(ValueError, "Incomplete snapshot"):
                build_features(spark, spec)

    result = None
    try:
        materialize(storage, source_root, source_identity, source_build)
        outcome = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(MLTests))
        if not outcome.wasSuccessful():
            raise RuntimeError(f"ML integration failed: {len(outcome.failures)} failures, {len(outcome.errors)} errors")
        result = {"passed": True, "tests_run": outcome.testsRun, "test_root": base,
                  "minimum_block_replicas": 2, "fault_scenarios": 7}
    finally:
        errors = []
        try:
            clear_mongo()
            if any(store.db[name].count_documents({"model_version": {"$in": list(owned_models)}})
                   for name in ("visitor_segments", "segment_profiles", "segment_models")):
                raise ValueError("Owned ML Mongo documents remain")
        except Exception as error:
            errors.append(type(error).__name__)
        try:
            storage.remove(base)
            if storage.exists(base):
                raise ValueError("Owned ML HDFS test root remains")
        except Exception as error:
            errors.append(type(error).__name__)
        if errors:
            raise RuntimeError("ML test cleanup failed: " + ", ".join(errors))
    return {**result, "cleaned": True}
