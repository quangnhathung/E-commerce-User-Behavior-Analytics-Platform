"""Publication boundaries against a stateful Mongo adapter, not mocked return values."""
import copy
import types
import unittest
from unittest import mock

from helpers import APP
from ecommerce_behavior.contracts import BEHAVIOR_VERSION, counts, product_fingerprint
from ecommerce_behavior.historical import status
from ecommerce_behavior.store import BehaviorStore
from ecommerce_streaming.store import Store
from test_streaming_store import Database, Replace


def snapshot(size=2):
    products = [{"item_id": str(index), **counts(3, 1, 1, 1, 1)} for index in range(size)]
    products.sort(key=lambda row: row["item_id"])
    return {"behavior_version": BEHAVIOR_VERSION, "scope_id": "test-run",
            **counts(3 * size, size, size, size, size), **product_fingerprint(products)}, products


class BehaviorStoreTests(unittest.TestCase):
    def setUp(self):
        patch = mock.patch.dict("sys.modules", {"pymongo": types.SimpleNamespace(ReplaceOne=Replace)})
        patch.start()
        self.addCleanup(patch.stop)
        self.db = Database()
        self.behavior = BehaviorStore(self.db)
        self.store = Store(self.db)
        self.config = {"run_id": "test-run", "initial_offsets": {"0": 0}, "pipeline_version": "stream-v2"}
        self.store.ensure_run(self.config)
        self.metrics = {"overview": {"processed_events": 6}, "minutes": []}
        self.metadata = {"worker_rows": {"spark-worker-1": 1, "spark-worker-2": 1}, "gold_path": "fixture"}

    def publish(self, batch, summary, products, fault=None):
        self.store.publish(self.config, batch, self.metrics, self.metadata, fault=fault, behavior=summary, products=iter(products))

    def test_partial_funnel_writes_keep_previous_kpi_and_funnel_pointer(self):
        summary, products = snapshot()
        self.publish(0, summary, products)
        next_summary, next_products = snapshot(101)

        def fault(stage):
            if stage == "mongo_mid_funnel_products":
                raise RuntimeError("partial funnel")

        with self.assertRaisesRegex(RuntimeError, "partial funnel"):
            self.publish(1, next_summary, next_products, fault)
        visible = self.store.status(self.config)
        self.assertEqual(0, visible["committed_batch_id"])
        self.assertEqual(summary, visible["funnel"]["overview"])
        self.assertIsNone(self.behavior.overview("test-run:1"))
        self.assertEqual(100, len(self.db.funnel_products.find({"snapshot_id": "test-run:1"})))
        self.publish(1, next_summary, next_products)
        self.assertEqual(next_summary, self.store.status(self.config)["funnel"]["overview"])

    def test_retry_after_pointer_preserves_product_count_and_values(self):
        summary, products = snapshot()

        def fault(stage):
            if stage == "after_mongo_publish":
                raise RuntimeError("after pointer")

        with self.assertRaises(RuntimeError):
            self.publish(0, summary, products, fault)
        previous = copy.deepcopy(self.db.funnel_products.documents)
        self.publish(0, summary, products)
        self.assertEqual(previous, self.db.funnel_products.documents)
        self.behavior.verify("test-run:0", summary)

    def test_wrong_product_content_cannot_publish_a_new_pointer(self):
        summary, products = snapshot()
        with self.assertRaisesRegex(ValueError, "differs"):
            self.publish(0, summary, products[:1])
        self.assertEqual(-1, self.store.load(self.config)["committed_batch_id"])

    def test_existing_snapshot_cannot_change_and_backward_publish_is_refused(self):
        summary, products = snapshot()
        self.publish(0, summary, products)
        changed, changed_products = snapshot(3)
        with self.assertRaisesRegex(ValueError, "existing funnel"):
            self.publish(0, changed, changed_products)
        self.publish(1, summary, products)
        with self.assertRaisesRegex(ValueError, "backwards"):
            self.publish(0, summary, products)

    def test_streaming_v2_refuses_kpi_without_funnel(self):
        with self.assertRaisesRegex(ValueError, "requires a funnel"):
            self.store.publish(self.config, 0, self.metrics, self.metadata)
        self.assertEqual({}, self.db.kpi_overview.documents)

    def test_corrupt_product_is_detected_on_readback(self):
        summary, products = snapshot()
        self.publish(0, summary, products)
        self.db.funnel_products.documents["test-run:0:0"]["view_to_cart_rate"] = .5
        with self.assertRaisesRegex(ValueError, "rate"):
            self.behavior.verify("test-run:0", summary)

    def test_page_is_bounded_sorted_and_exact_item_lookup_preserves_string(self):
        summary, products = snapshot(25)
        self.publish(0, summary, products)
        page = self.behavior.page("test-run:0", is_final=False)
        self.assertEqual(20, len(page["products"]))
        self.assertEqual(sorted(row["item_id"] for row in products)[:20], [row["item_id"] for row in page["products"]])
        self.assertEqual(["1"], [row["item_id"] for row in self.behavior.page("test-run:0", is_final=True, item_id="1")["products"]])
        self.assertEqual([], self.behavior.page("test-run:0", is_final=True, item_id="001")["products"])

    def test_final_flag_comes_from_registry_and_unpublished_history_is_hidden(self):
        summary, products = snapshot()
        self.publish(0, summary, products)
        self.assertFalse(self.store.status(self.config)["funnel"]["is_final"])
        self.store.finish(self.config, {"passed": True})
        self.assertTrue(self.store.status(self.config)["funnel"]["is_final"])
        self.db.behavior_runs.documents["history"] = {"_id": "history", "status": "prepared"}
        with self.assertRaisesRegex(ValueError, "not published"):
            status(self.store, "history")


if __name__ == "__main__":
    unittest.main()
