"""Live acceptance uses one UUID namespace and never removes business artifacts."""
from datetime import timedelta
import hashlib
import unittest
from unittest import mock
import uuid

from ecommerce_analytics.hdfs import Hdfs
from ecommerce_analytics.ingestion import silver_schema
from ecommerce_analytics.snapshots import materialize
from .contracts import (AUDIT, CANDIDATES, FEATURES, LABEL, choose_threshold, chunks, instant, iso,
                        labeled_reference, row_fingerprint, sample_reference)
from .data import build_chunk, day_path, finalize, load_dataset, ordered_rows, prepare
from .metrics import auc, evaluate, reference_metrics, threshold_metrics
from .scoring import score_batch
from .training import (finish_training, fit_pipeline, prepare_training, score_frame, train_candidate)


def fixture_events():
    result = []
    def add(visitor, item, kind, timestamp, transaction=None):
        result.append({"visitor_id": visitor, "item_id": item, "event_type": kind,
                       "event_time": iso(timestamp), "transaction_id": transaction})
    origin = instant("2015-05-10T12:00:00.000Z")
    for index in range(121):
        visitor = "001" if index == 120 else f"{index:04d}"
        for offset in range(1 + index % 6):
            add(visitor, str(offset % 3), "view", origin + timedelta(milliseconds=index * 10 + offset))
        if index % 3 == 0:
            add(visitor, "01", "add_to_cart", origin + timedelta(hours=1, milliseconds=index))
    for day in ("2015-05-11", "2015-05-13", "2015-05-15"):
        for visitor in ("0000", "001"):
            for item in ("01", "02"):
                add(visitor, item, "purchase", instant(day + "T00:00:00.000Z"), day.replace("-", "") + visitor)
    add("99998", "01", "view", instant("2015-05-03T00:00:00.000Z"))
    add("99999", "01", "view", instant("2015-05-18T00:00:00.000Z"))
    return result


def run_tests(spark, store, code_sha256):
    base = "/ecommerce/_tests/purchase/" + uuid.uuid4().hex
    print("PURCHASE_TEST_ROOT=" + base, flush=True)
    storage, root, cache = Hdfs(spark), base + "/silver", {}
    events = fixture_events()
    schedule = [{"split": split, "as_of": date + "T00:00:00.000Z"} for split, date in
                (("train", "2015-05-11"), ("validation", "2015-05-13"), ("test", "2015-05-15"))]
    identity = {"kind": "silver", "source": "retailrocket", "dataset_version": "a" * 64,
                "schema_version": 1, "transform_version": "events-v1"}
    def build_source(staging):
        rows = []
        for index, value in enumerate(events):
            timestamp = instant(value["event_time"])
            rows.append({**value, "event_time": timestamp, "event_date": timestamp.date(), "schema_version": 1,
                "dataset_version": "a" * 64, "source": "retailrocket", "run_id": "purchase-fixture",
                "event_id": hashlib.sha256(str(index).encode()).hexdigest(), "emitted_at": None,
                "source_file": "events.csv", "source_row_number": index + 2})
        spark.createDataFrame(rows, silver_schema()).repartition(4).write.partitionBy("event_date").parquet(staging + "/events")
        return {"quality": {"silver_rows": len(rows), "min_event_time": min(v["event_time"] for v in events),
                            "max_event_time": max(v["event_time"] for v in events)}}
    def fault_at(expected):
        def fault(actual):
            if actual == expected:
                raise RuntimeError("injected " + actual)
        return fault
    def score_request():
        return {"model_version": cache["training"]["model_version"], "as_of": "2015-05-16T00:00:00.000Z"}
    def clear_scores():
        ids = [row["score_run_id"] for row in store.db.purchase_score_runs.find({"identity.silver_path": root})]
        for name in ("purchase_scores", "purchase_score_runs"):
            store.db[name].delete_many({"score_run_id": {"$in": ids}})
            if store.db[name].count_documents({"score_run_id": {"$in": ids}}):
                raise ValueError("Purchase test score documents remain in " + name)

    class PurchaseTests(unittest.TestCase):
        def test_a_daily_labels_oracle_and_resume_without_partial_index(self):
            prepared = prepare(spark, {"silver_path": root, "dataset_version": "a" * 64, "base": base,
                                      "code_sha256": code_sha256, "schedule": schedule})
            config = cache["config"] = prepared["config"]
            first, *remaining = chunks(schedule)
            with self.assertRaisesRegex(RuntimeError, "injected"):
                build_chunk(spark, config, first, fault=fault_at("after_purchase_day"))
            self.assertFalse(storage.exists(config["root"] + "/index/_SUCCESS"))
            self.assertTrue(build_chunk(spark, config, first)["reused"])
            for entries in remaining:
                build_chunk(spark, config, entries)
            data = cache["data"] = finalize(spark, config)
            for entry in schedule:
                expected = row_fingerprint(labeled_reference(events, entry["as_of"]), labeled=True)
                actual = next(value["fingerprint"] for value in data["manifest"]["metadata"]["days"] if value["as_of"] == entry["as_of"])
                self.assertEqual(expected, actual)
                self.assertEqual(121, actual["rows"])
                self.assertEqual(2, actual["positives"])
            self.assertTrue(finalize(spark, config)["reused"])

        def test_b_hash_sample_matches_oracle_and_reuses(self):
            result = prepare_training(spark, store, cache["data"], code_sha256)
            training = cache["training"] = result["training"]
            frame = spark.read.parquet(training["sample"]["path"] + "/samples")
            expected = sample_reference(labeled_reference(events, schedule[0]["as_of"]), "a" * 64)
            self.assertEqual(row_fingerprint(expected, labeled=True), row_fingerprint(ordered_rows(frame), labeled=True))
            self.assertEqual(42, len(expected))
            self.assertTrue(prepare_training(spark, store, cache["data"], code_sha256)["sample_reused"])

        def test_c_all_candidates_selection_reload_test_and_reuse_without_refit(self):
            training = cache["training"]
            for parameters in CANDIDATES:
                self.assertFalse(train_candidate(spark, training, parameters, code_sha256)["reused"])
            with self.assertRaisesRegex(RuntimeError, "injected"):
                finish_training(spark, store, training, code_sha256, fault=fault_at("before_purchase_model"))
            first = cache["model"] = finish_training(spark, store, training, code_sha256)
            self.assertTrue(first["report"]["evaluation"]["reload_verified"])
            self.assertEqual(121, first["report"]["evaluation"]["test_fingerprint"]["summary"]["rows"])
            self.assertEqual("published", store.model_status(training["model_version"])["status"])
            with mock.patch(__package__ + ".training.fit_pipeline", side_effect=AssertionError("refit")):
                for parameters in CANDIDATES:
                    self.assertTrue(train_candidate(spark, training, parameters, code_sha256)["reused"])
                self.assertTrue(finish_training(spark, store, training, code_sha256)["reused"])

        def test_d_exact_metrics_threshold_grid_and_daily_ties_match_oracle(self):
            from pyspark.sql import functions as F
            frame = spark.read.parquet(day_path(cache["config"], schedule[1]) + "/samples")
            frame = frame.withColumn("purchase_score", F.when(F.col("visitor_id") == "0000", .9)
                                     .when(F.col("visitor_id") == "001", .5).otherwise(.1))
            for scored in (frame, frame.withColumn("purchase_score", F.lit(.5))):
                expected = reference_metrics([row.asDict() for row in scored.collect()], .5)
                actual = evaluate(spark, scored, .5)
                for key in expected:
                    if isinstance(expected[key], float):
                        self.assertAlmostEqual(expected[key], actual[key], places=12, msg=key)
                    else:
                        self.assertEqual(expected[key], actual[key], key)
                grid = threshold_metrics(scored)
                self.assertEqual(101, len(grid))
                self.assertIn(choose_threshold(grid)["threshold"], [value["threshold"] for value in grid])

        def test_e_score_publish_faults_retry_lookup_and_no_duplicates(self):
            first = cache["score"] = score_batch(spark, store, score_request(), code_sha256)
            self.assertEqual(121, first["quality"]["fingerprint"]["rows"])
            self.assertEqual("001", store.score_status(first["score_run_id"], "001")["visitor"]["visitor_id"])
            self.assertIsNone(store.score_status(first["score_run_id"], "1")["visitor"])
            for stage in ("after_purchase_score_gold", "mongo_mid_purchase_scores", "before_purchase_score_pointer", "after_purchase_score_pointer"):
                with self.subTest(stage=stage):
                    clear_scores()
                    with self.assertRaisesRegex(RuntimeError, "injected"):
                        score_batch(spark, store, score_request(), code_sha256, fault=fault_at(stage))
                    if stage == "after_purchase_score_pointer":
                        self.assertEqual("published", store.score_status(first["score_run_id"])["status"])
                    else:
                        with self.assertRaisesRegex(ValueError, "not published"):
                            store.score_status(first["score_run_id"])
                    self.assertTrue(score_batch(spark, store, score_request(), code_sha256)["reused"])
                    self.assertEqual(121, store.db.purchase_scores.count_documents({"score_run_id": first["score_run_id"]}))

        def test_f_constant_features_single_class_and_source_compatibility(self):
            from pyspark.sql import functions as F
            sampled = spark.read.parquet(cache["training"]["sample"]["path"] + "/samples")
            first = sampled.first().asDict()
            constant = sampled
            for key in FEATURES:
                constant = constant.withColumn(key, F.lit(first[key]))
            model = fit_pipeline(spark, constant, CANDIDATES[0])
            self.assertTrue(all(0 <= row[0] <= 1 for row in score_frame(spark, model, constant).select("purchase_score").collect()))
            with self.assertRaisesRegex(ValueError, "both classes"):
                auc(spark, constant.withColumn(LABEL, F.lit(0)).withColumn("purchase_score", F.lit(.5)))
            with self.assertRaisesRegex(ValueError, "code/Spark"):
                score_batch(spark, store, score_request(), "b" * 64)
            with self.assertRaisesRegex(ValueError, "first test"):
                score_batch(spark, store, {**score_request(), "as_of": schedule[1]["as_of"]}, code_sha256)

        def test_g_missing_markers_and_corrupt_model_are_refused(self):
            model_path = cache["training"]["model_path"]
            storage.write_bytes(model_path + "/unexpected-corrupt-file", b"bad")
            with self.assertRaisesRegex(ValueError, "inventory mismatch"):
                score_batch(spark, store, score_request(), code_sha256)
            storage.remove(model_path + "/unexpected-corrupt-file")
            storage.remove(day_path(cache["config"], schedule[0]) + "/_SUCCESS")
            with self.assertRaisesRegex(ValueError, "Incomplete snapshot"):
                load_dataset(spark, storage, cache["data"], code_sha256)

    result = None
    try:
        materialize(storage, root, identity, build_source)
        outcome = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(PurchaseTests))
        if not outcome.wasSuccessful():
            raise RuntimeError(f"Purchase integration failed: {len(outcome.failures)} failures, {len(outcome.errors)} errors")
        result = {"passed": True, "tests_run": outcome.testsRun, "fault_scenarios": 6,
                  "test_root": base, "minimum_block_replicas": 2}
    finally:
        errors = []
        try:
            clear_scores()
            models = [value["model_version"] for value in store.db.purchase_models.find({"data_path": cache.get("config", {}).get("root", base) + "/index"})]
            store.db.purchase_models.delete_many({"model_version": {"$in": models}})
            if store.db.purchase_score_runs.count_documents({"identity.silver_path": root}) or store.db.purchase_models.count_documents({"model_version": {"$in": models}}):
                raise ValueError("Purchase test documents remain")
        except Exception as error:
            errors.append(type(error).__name__)
        try:
            storage.remove(base)
            if storage.exists(base):
                raise ValueError("Purchase HDFS test root remains")
        except Exception as error:
            errors.append(type(error).__name__)
        if errors:
            raise RuntimeError("Purchase test cleanup failed: " + ", ".join(errors))
    return {**result, "cleaned": True}
