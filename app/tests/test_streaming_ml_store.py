"""Partial writes, atomic run visibility and immutable published ML rows."""
import copy
import types
import unittest
from unittest import mock

from helpers import APP
from test_streaming_store import Database, Replace
from test_streaming_ml import bindings, predictions, request
from ecommerce_behavior.contracts import reference
from ecommerce_streaming_ml.contracts import DEFAULT_START, cutoff, prediction_fingerprint, simulator_events
from ecommerce_streaming_ml.store import Store


class StreamingMLStoreTests(unittest.TestCase):
    def setUp(self):
        patch = mock.patch.dict("sys.modules", {"pymongo": types.SimpleNamespace(ReplaceOne=Replace)})
        patch.start(); self.addCleanup(patch.stop)
        self.db, self.rows = Database(), predictions()
        self.store = Store(self.db)
        self.config = {**request(), "initial_offsets": {"0": 0}, "bindings": bindings()}
        self.store.ensure_run(self.config)
        self.quality = {"status": "ready", "as_of": cutoff(DEFAULT_START), "scored_at": DEFAULT_START,
            "fingerprint": prediction_fingerprint(self.rows, bindings(), cutoff(DEFAULT_START), DEFAULT_START)}
        self.behavior = reference([{"event_id": str(index), "visitor_id": row["visitor_id"], "item_id": "10",
            "event_type": "view", "transaction_id": None, "event_time": DEFAULT_START}
            for index, row in enumerate(self.rows)], request()["run_id"])
        self.metrics = {"overview": {"processed_events": 121, "visitors": 121}, "minutes": []}
        self.metadata = {"worker_rows": {"spark-worker-1": 1, "spark-worker-2": 1},
                         "processed_offsets": {"0": 9}, "counts": {"accepted": 9}}

    def publish(self, batch_id, fault=None):
        return self.store.publish(self.config, batch_id, self.metrics, self.metadata, fault,
            behavior=self.behavior["summary"], products=iter(self.behavior["products"]),
            ml=self.quality, predictions=iter(self.rows))

    def test_partial_ml_is_hidden_and_retry_does_not_duplicate(self):
        def fail(stage):
            if stage == "mongo_mid_ml": raise RuntimeError("injected")
        with self.assertRaises(RuntimeError): self.publish(0, fail)
        self.assertEqual(100, len(self.db.stream_ml_predictions.documents))
        self.assertEqual(-1, self.store.status(self.config)["committed_batch_id"])
        self.publish(0); self.publish(0)
        self.assertEqual(121, len(self.db.stream_ml_predictions.documents))
        state = self.store.status(self.config, "001")
        self.assertEqual(0, state["committed_batch_id"]); self.assertEqual("001", state["visitor"]["visitor_id"])
        self.assertEqual(self.quality["fingerprint"], state["scoring"]["fingerprint"])

    def test_fault_before_and_after_run_pointer_is_recoverable(self):
        for stage in ("mongo_before_pointer", "after_mongo_publish"):
            with self.subTest(stage=stage):
                def fault(actual):
                    if stage == actual: raise RuntimeError("injected")
                with self.assertRaises(RuntimeError): self.publish(0, fault)
                expected = -1 if stage == "mongo_before_pointer" else 0
                self.assertEqual(expected, self.store.status(self.config)["committed_batch_id"])
                self.publish(0)
                self.assertEqual(121, len(self.db.stream_ml_predictions.documents))

    def test_published_corruption_is_refused_without_healing(self):
        self.publish(0)
        identity = self.config["run_id"] + ":0:" + self.rows[0]["visitor_id"]
        self.db.stream_ml_predictions.documents[identity]["purchase_score"] = .1
        with self.assertRaises(ValueError): self.publish(0)
        self.assertEqual(.1, self.db.stream_ml_predictions.documents[identity]["purchase_score"])

    def test_newer_partial_batch_cannot_change_previous_snapshot(self):
        self.publish(0)
        def fault(stage):
            if stage == "mongo_before_pointer": raise RuntimeError("injected")
        with self.assertRaises(RuntimeError): self.publish(1, fault)
        self.assertEqual(0, self.store.status(self.config)["committed_batch_id"])
        self.publish(1)
        with self.assertRaisesRegex(ValueError, "backwards"): self.publish(0)

    def test_bound_config_and_quality_are_immutable(self):
        self.publish(0)
        changed = copy.deepcopy(self.config); changed["bindings"]["threshold"] = .5
        with self.assertRaises(ValueError): self.store.status(changed)
        self.quality["as_of"] = DEFAULT_START
        with self.assertRaisesRegex(ValueError, "immutable"): self.publish(0)

    def test_missing_ml_or_funnel_never_publishes(self):
        with self.assertRaises(ValueError):
            self.store.publish(self.config, 0, self.metrics, self.metadata)
        self.assertEqual(-1, self.store.status(self.config)["committed_batch_id"])
