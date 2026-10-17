"""Live saved-score fixtures: no feature construction, fitting or inference."""
from datetime import timedelta
import unittest
from unittest import mock
import uuid

from ecommerce_analytics.hdfs import Hdfs
from ecommerce_analytics.snapshots import materialize, write_json
from ecommerce_prediction.contracts import (AUDIT, CANDIDATES, DATA_VERSION, FEATURES, FEATURE_VERSION,
    LABEL, METRIC_VERSION, MODEL_VERSION, SCORE_VERSION, composite, digest, instant, row_fingerprint)
from ecommerce_prediction.metrics import evaluate, threshold_metrics
from ecommerce_prediction.training import fingerprints
from .workflow import apply, tune


def rows_for(as_of, *, mode="normal"):
    result = []
    for index in range(121):
        score = .96 if index < 8 or 10 <= index < 12 else .94 if index < 17 else .1
        if mode == "infeasible":
            score = .5
        elif mode == "test_fail":
            score = .96 if index < 3 or 10 <= index < 21 else .94 if index < 10 else .1
        elif mode == "zero":
            score = .1
        result.append({"visitor_id": "001" if index == 120 else f"{index:04d}", "as_of": instant(as_of),
            "last_event_time": instant(as_of) - timedelta(hours=2),
            "view_count_7d": 1, "cart_count_7d": 0, "transaction_count_7d": 0,
            "unique_items_7d": 1, "active_days_7d": 1, "recency_hours": 2.0,
            "cart_view_ratio_7d": 0.0, "transaction_view_ratio_7d": 0.0,
            LABEL: int(index < 10), "purchase_score": score, "predicted_label": int(score >= .93)})
    return sorted(result, key=lambda row: row["visitor_id"])


def score_schema():
    from pyspark.sql import types as T
    return T.StructType([T.StructField("visitor_id", T.StringType()), T.StructField("as_of", T.TimestampType()),
        T.StructField("last_event_time", T.TimestampType()),
        *[T.StructField(key, T.LongType()) for key in FEATURES[:5]],
        *[T.StructField(key, T.DoubleType()) for key in FEATURES[5:]], T.StructField(LABEL, T.IntegerType()),
        T.StructField("purchase_score", T.DoubleType()), T.StructField("predicted_label", T.IntegerType())])


def seed_scores(spark, store, base, variant):
    """Construct published provenance plus typed predictions, never a trained RF."""
    storage = Hdfs(spark)
    base = base + "/" + variant
    schedule = [{"split": name, "as_of": date + "T00:00:00.000Z"} for name, date in
                (("train", "2015-05-11"), ("validation", "2015-05-13"), ("test", "2015-05-15"))]
    config_identity = {"kind": "purchase-data", "data_version": DATA_VERSION, "feature_version": FEATURE_VERSION,
        "schedule": schedule, "code_sha256": "f" * 64, "silver_path": base + "/silver", "fixture": variant}
    dataset_id = digest(config_identity)
    config = {"identity": config_identity, "purchase_dataset_id": dataset_id, "base": base,
              "root": base + "/gold/purchase-data/" + dataset_id}
    frames, rows, days = {}, {}, []
    for entry in schedule:
        mode = "infeasible" if variant == "infeasible" else "test_fail" if variant == "test_fail" and entry["split"] == "test" else "normal"
        values = rows_for(entry["as_of"], mode=mode)
        rows[entry["split"]] = values
        frames[entry["split"]] = spark.createDataFrame(values, score_schema())
        days.append({**entry, "path": config["root"] + "/days/" + entry["as_of"][:10],
                     "fingerprint": row_fingerprint(values, labeled=True), "manifest_sha256": "a" * 64})
    index = {"passed": True, "days": days, "feature_order": list(FEATURES),
             "splits": {name: composite([day for day in days if day["split"] == name]) for name in frames}}
    def metadata_snapshot(path, identity, filename, metadata, extra=None):
        def build(staging):
            if extra:
                extra(staging)
            write_json(storage, staging + "/" + filename, metadata)
            return metadata
        materialize(storage, path, identity, build)
        return storage.sha256(path + "/manifest.json")
    data_path = config["root"] + "/index"
    data_hash = metadata_snapshot(data_path, {"kind": "purchase-index", "config": config}, "index.json", index)
    model_identity = {"kind": "purchase-model", "model_contract": MODEL_VERSION, "feature_order": list(FEATURES),
        "spark_version": spark.version, "code_sha256": "f" * 64, "purchase_dataset_id": dataset_id,
        "data_manifest_sha256": data_hash, "fixture_saved_scores_only": True}
    version = digest(model_identity)
    candidate_path = base + "/models/purchase-candidates/" + version + "/fixture"
    validation_metrics = evaluate(spark, frames["validation"], .93)
    candidate = {"passed": True, "parameters": CANDIDATES[0], "metrics": {key: validation_metrics[key] for key in ("pr_auc", "roc_auc")},
        "feature_importance": dict.fromkeys(FEATURES, .125), "validation_fingerprint": fingerprints(frames["validation"], [schedule[1]])}
    candidate_hash = metadata_snapshot(candidate_path, {"kind": "purchase-candidate", "model_version": version,
        "parameters": CANDIDATES[0]}, "quality.json", candidate,
        lambda staging: frames["validation"].write.parquet(staging + "/validation_scores"))
    winner = {**candidate, "candidate_path": candidate_path, "manifest_sha256": candidate_hash}
    selection = {"threshold": .93, "winner": winner, "candidates": [winner], "selected_parameters": CANDIDATES[0],
        "threshold_grid": threshold_metrics(frames["validation"]), "validation_metrics": validation_metrics,
        "feature_order": list(FEATURES), "feature_importance": candidate["feature_importance"]}
    model_path = base + "/models/purchases/" + version
    model_hash = metadata_snapshot(model_path, model_identity, "selection.json", selection,
        lambda staging: storage.write_bytes(staging + "/pipeline-must-never-be-loaded.txt", b"Saved scalar fixture, not an RF pipeline"))
    evaluation = {"passed": True, "fixture_saved_scores_only": True, "test": evaluate(spark, frames["test"], .93),
                  "test_fingerprint": fingerprints(frames["test"], [schedule[2]], threshold=.93)}
    evaluation_identity = {"kind": "purchase-evaluation", "model_version": version,
        "model_manifest_sha256": model_hash, "data_manifest_sha256": data_hash, "metric_version": METRIC_VERSION}
    evaluation_path = base + "/gold/purchase-evaluations/" + digest(evaluation_identity)
    evaluation_hash = metadata_snapshot(evaluation_path, evaluation_identity, "evaluation.json", evaluation,
        lambda staging: frames["test"].write.parquet(staging + "/test_scores"))
    training = {"identity": model_identity, "model_version": version, "model_path": model_path, "data": {"data_path": data_path}}
    store.publish_model(training, {"selection": selection, "evaluation": evaluation, "model_manifest_sha256": model_hash,
        "evaluation_manifest_sha256": evaluation_hash, "evaluation_path": evaluation_path})
    bundle = {"base": base, "model_version": version, "model_hash": model_hash, "model_path": model_path,
              "evaluation_path": evaluation_path, "candidate_path": candidate_path, "training": training}
    bundle["score_run_id"] = seed_batch(spark, store, bundle)
    return bundle


def seed_batch(spark, store, bundle, *, zero=False):
    storage = Hdfs(spark)
    as_of = "2015-05-17T00:00:00.000Z" if zero else "2015-05-16T00:00:00.000Z"
    rows = rows_for(as_of, mode="zero" if zero else "normal")
    frame = spark.createDataFrame(rows, score_schema()).select(*AUDIT, *FEATURES, "purchase_score", "predicted_label")
    identity = {"kind": "purchase-score", "score_version": SCORE_VERSION, "as_of": as_of,
        "model_version": bundle["model_version"], "model_manifest_sha256": bundle["model_hash"],
        "code_sha256": "f" * 64, "silver_path": bundle["base"] + "/silver", "threshold": .93}
    key = digest(identity)
    path = bundle["base"] + "/gold/purchase-scores/" + key
    quality = {"passed": True, "as_of": as_of, "threshold": .93, "feature_order": list(FEATURES),
               "fingerprint": row_fingerprint(rows, scored=True, threshold=.93)}
    def build(staging):
        frame.write.parquet(staging + "/scores")
        write_json(storage, staging + "/quality.json", quality)
        return quality
    materialize(storage, path, identity, build)
    store.publish_scores(identity, path, quality, iter(rows))
    return key


def run_tests(spark, store, code_sha256):
    base = "/ecommerce/_tests/purchase-policy/" + uuid.uuid4().hex
    print("PURCHASE_POLICY_TEST_ROOT=" + base, flush=True)
    storage, cache = Hdfs(spark), {}
    def fault_at(expected):
        def fault(actual):
            if actual == expected:
                raise RuntimeError("injected " + actual)
        return fault
    class PolicyTests(unittest.TestCase):
        def test_a_selection_commits_before_test_and_reuse_never_recomputes(self):
            bundle = cache["normal"] = seed_scores(spark, store, base, "normal")
            with self.assertRaisesRegex(RuntimeError, "injected"):
                tune(spark, store, bundle["model_version"], code_sha256, fault=fault_at("after_policy_selection"))
            policy = store.db.purchase_policies.find_one({"model_version": bundle["model_version"]})
            self.assertTrue(storage.exists(policy["selection_path"] + "/_SUCCESS"))
            self.assertFalse(storage.exists(policy["evaluation_path"] + "/_SUCCESS"))
            with self.assertRaises(ValueError):
                store.policy_status(policy["policy_version"])
            with self.assertRaisesRegex(RuntimeError, "injected"):
                tune(spark, store, bundle["model_version"], code_sha256, fault=fault_at("before_policy_pointer"))
            first = cache["policy"] = tune(spark, store, bundle["model_version"], code_sha256)
            self.assertTrue(first["eligible"])
            self.assertEqual(.96, first["threshold"])
            self.assertFalse(first["report"]["evaluation"]["fresh_holdout"])
            with mock.patch(__package__ + ".workflow.threshold_metrics", side_effect=AssertionError("recomputed")):
                self.assertTrue(tune(spark, store, bundle["model_version"], code_sha256)["reused"])

        def test_b_infeasible_validation_and_failed_test_block_application(self):
            for variant in ("infeasible", "test_fail"):
                bundle = cache[variant] = seed_scores(spark, store, base, variant)
                policy = tune(spark, store, bundle["model_version"], code_sha256)
                self.assertFalse(policy["eligible"])
                self.assertEqual(variant == "infeasible", policy["report"]["evaluation"]["test_skipped"])
                with self.assertRaisesRegex(ValueError, "not eligible"):
                    apply(spark, store, policy["policy_version"], bundle["score_run_id"], code_sha256)

        def test_c_apply_faults_retry_lookup_and_original_immutable(self):
            bundle, policy = cache["normal"], cache["policy"]["policy_version"]
            before = store.score_status(bundle["score_run_id"])
            first = cache["score"] = apply(spark, store, policy, bundle["score_run_id"], code_sha256)
            key = first["score_run_id"]
            self.assertEqual(121, first["quality"]["fingerprint"]["rows"])
            self.assertTrue(first["quality"]["scores_unchanged"])
            self.assertEqual(policy, store.score_status(key, "001")["visitor"]["policy_version"])
            self.assertIsNone(store.score_status(key, "1")["visitor"])
            for stage in ("after_policy_score_gold", "mongo_mid_policy_scores", "before_policy_score_pointer", "after_policy_score_pointer"):
                for name in ("purchase_scores", "purchase_score_runs"):
                    store.db[name].delete_many({"score_run_id": key})
                with self.assertRaisesRegex(RuntimeError, "injected"):
                    apply(spark, store, policy, bundle["score_run_id"], code_sha256, fault=fault_at(stage))
                if stage != "after_policy_score_pointer":
                    with self.assertRaises(ValueError):
                        store.score_status(key)
                self.assertTrue(apply(spark, store, policy, bundle["score_run_id"], code_sha256)["reused"])
                self.assertEqual(121, store.db.purchase_scores.count_documents({"score_run_id": key}))
            self.assertEqual(before, store.score_status(bundle["score_run_id"]))
            store.verify_scores(bundle["score_run_id"], before["report"], .93)

        def test_d_wrong_model_code_nested_policy_and_corruption_refused(self):
            policy, bundle = cache["policy"]["policy_version"], cache["normal"]
            with self.assertRaisesRegex(ValueError, "model mismatch"):
                apply(spark, store, policy, cache["test_fail"]["score_run_id"], code_sha256)
            with self.assertRaisesRegex(ValueError, "code/Spark"):
                apply(spark, store, policy, bundle["score_run_id"], "b" * 64)
            with self.assertRaisesRegex(ValueError, "cannot be nested"):
                apply(spark, store, policy, cache["score"]["score_run_id"], code_sha256)
            path = store.policy_status(policy)["evaluation_path"]
            storage.write_bytes(path + "/corrupt-extra", b"bad")
            try:
                with self.assertRaisesRegex(ValueError, "inventory mismatch"):
                    apply(spark, store, policy, bundle["score_run_id"], code_sha256)
            finally:
                storage.remove(path + "/corrupt-extra")

        def test_e_zero_positive_batch_is_valid_and_keeps_all_scores(self):
            source = seed_batch(spark, store, cache["normal"], zero=True)
            result = apply(spark, store, cache["policy"]["policy_version"], source, code_sha256)
            self.assertEqual(0, result["quality"]["fingerprint"]["predicted_positive"])
            self.assertEqual(121, result["quality"]["fingerprint"]["rows"])

    try:
        with mock.patch("ecommerce_prediction.training.fit_pipeline", side_effect=AssertionError("fit forbidden")), \
                mock.patch("ecommerce_prediction.data.labeled_frame", side_effect=AssertionError("feature build forbidden")), \
                mock.patch("ecommerce_ml.native.NativeML.load", side_effect=AssertionError("inference forbidden")):
            outcome = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyTests))
        if not outcome.wasSuccessful():
            raise RuntimeError(f"Policy integration failed: {len(outcome.failures)} failures, {len(outcome.errors)} errors")
        report = {"passed": True, "tests_run": outcome.testsRun, "fault_scenarios": 6,
                  "test_root": base, "minimum_block_replicas": 2}
    finally:
        errors = []
        try:
            models = [value["model_version"] for value in store.db.purchase_models.find({})
                      if value["model_path"].startswith(base + "/")]
            for name in ("purchase_scores", "purchase_score_runs", "purchase_policies", "purchase_models"):
                store.db[name].delete_many({"model_version": {"$in": models}})
                if store.db[name].count_documents({"model_version": {"$in": models}}):
                    raise ValueError("Policy test documents remain in " + name)
        except Exception as error:
            errors.append(type(error).__name__)
        try:
            storage.remove(base)
            if storage.exists(base):
                raise ValueError("Policy test HDFS root remains")
        except Exception as error:
            errors.append(type(error).__name__)
        if errors:
            raise RuntimeError("Policy test cleanup failed: " + ", ".join(errors))
    return {**report, "cleaned": True}
