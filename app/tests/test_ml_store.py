"""Actual partial writes/retries through a stateful Mongo test adapter."""
import copy
from datetime import timedelta
import types
import unittest
from unittest import mock

from helpers import APP
from ecommerce_ml.contracts import digest, fingerprint, reference, window, DEFAULT_AS_OF
from ecommerce_ml.store import Store
from test_ml import event
from test_streaming_store import Database, Replace


class MLStoreTests(unittest.TestCase):
    def setUp(self):
        patch = mock.patch.dict("sys.modules", {"pymongo": types.SimpleNamespace(ReplaceOne=Replace)})
        patch.start()
        self.addCleanup(patch.stop)
        self.db, self.identity = Database(), {"kind": "segments", "code_sha256": "a" * 64}
        self.store = Store(self.db)
        self.version = digest(self.identity)
        start, _ = window(DEFAULT_AS_OF)
        self.rows = [{**row, "cluster_id": 0} for row in reference([
            event(start + timedelta(milliseconds=index), visitor_id=f"{index:04d}") for index in range(121)])]
        self.report = {"assignment_fingerprint": fingerprint(self.rows, assignments=True),
                       "profiles": [{"cluster_id": 0, "count": 121, "raw_feature_means": {"view_count_7d": 1.0}}]}
        self.store.prepare(self.version, self.identity, "/model", False)

    def publish(self, fault=None):
        self.store.publish(self.version, self.identity, "/model", self.report, iter(self.rows), fault=fault)

    def test_partial_mongo_is_hidden_and_retry_fills_without_duplicates(self):
        def fault(stage):
            if stage == "mongo_mid_assignments":
                raise RuntimeError("partial")
        with self.assertRaisesRegex(RuntimeError, "partial"):
            self.publish(fault)
        self.assertEqual(100, len(self.db.visitor_segments.documents))
        with self.assertRaisesRegex(ValueError, "not published"):
            self.store.status(self.version)
        self.publish()
        self.assertEqual(121, len(self.db.visitor_segments.documents))
        self.store.verify(self.version, self.report)

    def test_faults_before_and_after_pointer_and_immutable_retry(self):
        for stage in ("before_model_pointer", "after_model_pointer"):
            with self.subTest(stage=stage):
                def fault(actual):
                    if actual == stage:
                        raise RuntimeError("injected")
                with self.assertRaisesRegex(RuntimeError, "injected"):
                    self.publish(fault)
                if stage == "before_model_pointer":
                    with self.assertRaises(ValueError):
                        self.store.status(self.version)
                else:
                    self.assertEqual("published", self.store.status(self.version)["status"])
                original = copy.deepcopy(self.db.visitor_segments.documents)
                self.publish()
                self.assertEqual(original, self.db.visitor_segments.documents)
                if stage == "before_model_pointer":
                    # Start the next fault case from a prepared registry of the same owned fixture.
                    self.db.segment_models.documents[self.version]["status"] = "prepared"

    def test_wrong_coverage_or_content_cannot_publish(self):
        self.rows.pop()
        with self.assertRaisesRegex(ValueError, "fingerprint"):
            self.publish()
        with self.assertRaises(ValueError):
            self.store.status(self.version)

    def test_corrupt_published_assignment_is_refused_without_repairing(self):
        self.publish()
        self.db.visitor_segments.documents[self.version + ":0000"]["cluster_id"] = 99
        with self.assertRaisesRegex(ValueError, "fingerprint"):
            self.publish()
        self.assertEqual(99, self.db.visitor_segments.documents[self.version + ":0000"]["cluster_id"])

    def test_lookup_is_optional_exact_and_preserves_leading_zero(self):
        self.publish()
        self.assertNotIn("visitor", self.store.status(self.version))
        self.assertEqual("0001", self.store.status(self.version, "0001")["visitor"]["visitor_id"])
        self.assertIsNone(self.store.status(self.version, "1")["visitor"])

    def test_changed_identity_report_and_missing_published_gold_are_refused(self):
        self.publish()
        with self.assertRaisesRegex(ValueError, "missing"):
            self.store.prepare(self.version, self.identity, "/model", False)
        with self.assertRaisesRegex(ValueError, "identity"):
            self.store.prepare(self.version, self.identity, "/different", True)
        self.report["changed"] = True
        with self.assertRaisesRegex(ValueError, "published model report"):
            self.publish()
