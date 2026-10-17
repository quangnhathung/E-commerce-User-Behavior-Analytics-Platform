"""Exercise snapshot visibility and retry against a small stateful Mongo boundary."""
import copy
import types
import unittest
from unittest import mock

from helpers import APP
from ecommerce_streaming.store import Store


class Replace:
    def __init__(self, query, document, upsert):
        self.query, self.document = query, document


class Collection:
    def __init__(self):
        self.documents = {}

    def create_index(self, keys):
        pass

    def find_one(self, query, projection=None):
        result = copy.deepcopy(self.documents.get(query["_id"]))
        if result is not None and projection:
            for key, included in projection.items():
                if not included:
                    result.pop(key, None)
        return result

    def update_one(self, query, changes, upsert=False):
        identity = query["_id"]
        new = identity not in self.documents
        if new and not upsert:
            return types.SimpleNamespace(matched_count=0)
        if "committed_batch_id" in query and self.documents[identity]["committed_batch_id"] > query["committed_batch_id"]["$lte"]:
            return types.SimpleNamespace(matched_count=0)
        document = self.documents.setdefault(identity, {"_id": identity})
        if new:
            document.update(copy.deepcopy(changes.get("$setOnInsert", {})))
        for key, value in changes.get("$set", {}).items():
            target = document
            *parents, leaf = key.split(".")
            for part in parents:
                target = target.setdefault(part, {})
            target[leaf] = copy.deepcopy(value)
        for key, value in changes.get("$addToSet", {}).items():
            for member in value["$each"]:
                if member not in document[key]:
                    document[key].append(member)
        return types.SimpleNamespace(matched_count=1)

    def replace_one(self, query, document, upsert=False):
        self.documents[query["_id"]] = copy.deepcopy(document)

    def bulk_write(self, operations, ordered):
        for operation in operations:
            self.replace_one(operation.query, operation.document, upsert=True)


class Database:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        return self.collections.setdefault(name, Collection())

    def __getattr__(self, name):
        return self[name]


class StoreTests(unittest.TestCase):
    def setUp(self):
        module = types.ModuleType("pymongo")
        module.ReplaceOne = Replace
        patch = mock.patch.dict("sys.modules", {"pymongo": module})
        patch.start()
        self.addCleanup(patch.stop)
        self.db = Database()
        self.store = Store(self.db)
        self.config = {"run_id": "test-run", "initial_offsets": {"0": 0}}
        self.store.ensure_run(self.config)
        self.metadata = {"worker_rows": {"spark-worker-1": 1, "spark-worker-2": 1},
                         "processed_offsets": {"0": 2}, "counts": {"accepted": 2}}
        self.first = {"overview": {"processed_events": 1}, "minutes": [{"window_start": "one", "processed_events": 1}]}
        self.second = {"overview": {"processed_events": 2}, "minutes": [
            {"window_start": "one", "processed_events": 1}, {"window_start": "two", "processed_events": 1}]}

    def test_partial_minute_writes_do_not_publish_pending_snapshot(self):
        self.store.publish(self.config, 0, self.first, self.metadata)

        def fail(stage):
            if stage == "mongo_mid_minutes":
                raise RuntimeError("injected partial minute write")

        with self.assertRaises(RuntimeError):
            self.store.publish(self.config, 1, self.second, self.metadata, fault=fail)
        visible = self.store.status(self.config)
        self.assertEqual(0, visible["committed_batch_id"])
        self.assertEqual({"processed_events": 1}, visible["overview"])
        self.assertIn("test-run:1:one", self.db.kpi_minute.documents)
        self.assertNotIn("test-run:1:two", self.db.kpi_minute.documents)
        self.store.publish(self.config, 1, self.second, self.metadata)
        self.assertEqual({"processed_events": 2}, self.store.status(self.config)["overview"])

    def test_retry_after_pointer_does_not_duplicate_kpi_documents(self):
        def fail(stage):
            if stage == "after_mongo_publish":
                raise RuntimeError("injected after pointer")

        with self.assertRaises(RuntimeError):
            self.store.publish(self.config, 0, self.second, self.metadata, fault=fail)
        self.store.publish(self.config, 0, self.second, self.metadata)
        self.assertEqual(1, len(self.db.kpi_overview.documents))
        self.assertEqual(2, len(self.db.kpi_minute.documents))
        self.assertEqual({"processed_events": 2}, self.store.status(self.config)["overview"])

    def test_registry_mismatch_and_backwards_pointer_are_refused(self):
        with self.assertRaises(ValueError):
            self.store.load({**self.config, "initial_offsets": {"0": 99}})
        self.store.publish(self.config, 1, self.second, self.metadata)
        with self.assertRaisesRegex(ValueError, "backwards"):
            self.store.publish(self.config, 0, self.first, self.metadata)
        self.assertEqual(1, self.store.status(self.config)["committed_batch_id"])

    def test_producer_state_does_not_overwrite_consumer_or_configuration(self):
        self.store.consumer_status(self.config, "running")
        self.store.producer_progress(self.config, {"last_seq": 1, "end_offsets": {"0": 2}}, "completed")
        run = self.store.load(self.config)
        self.assertEqual("running", run["consumer"]["status"])
        self.assertEqual("completed", run["producer"]["status"])
        self.assertEqual(self.config, run["config"])


if __name__ == "__main__":
    unittest.main()
