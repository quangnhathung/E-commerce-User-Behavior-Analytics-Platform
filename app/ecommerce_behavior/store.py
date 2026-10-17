"""Idempotent funnel documents. Callers publish their registry pointer last."""
from .contracts import BEHAVIOR_VERSION, product_fingerprint, validate_counts

PROJECTION = {"_id": 0, "snapshot_id": 0, "run_id": 0, "batch_id": 0, "analysis_id": 0}


class BehaviorStore:
    def __init__(self, database):
        self.db = database

    def indexes(self):
        self.db.funnel_products.create_index([("snapshot_id", 1), ("item_id", 1)], unique=True)
        self.db.funnel_products.create_index([("snapshot_id", 1), ("view_sessions", -1), ("item_id", 1)])

    def publish(self, snapshot_id, summary, products, ownership, fault=None):
        from pymongo import ReplaceOne
        validate_counts(summary)
        if summary["behavior_version"] != BEHAVIOR_VERSION:
            raise ValueError("Unsupported behavior version")
        previous = self.db.funnel_overview.find_one({"_id": snapshot_id}, PROJECTION)
        if previous is not None and previous != summary:
            raise ValueError("Refusing to change an existing funnel snapshot")
        pending, event_counts = [], [0]

        def flush():
            if pending:
                self.db.funnel_products.bulk_write(pending, ordered=True)
                pending.clear()
                if fault:
                    fault("mongo_mid_funnel_products")

        def rows():
            for row in products:
                yield row  # Validate before adding this row to the write batch.
                event_counts[0] += row["event_count"]
                key = snapshot_id + ":" + row["item_id"]
                pending.append(ReplaceOne({"_id": key}, {
                    **row, **ownership, "_id": key, "snapshot_id": snapshot_id}, upsert=True))
                if len(pending) == 100:
                    flush()
            flush()

        actual = product_fingerprint(rows())
        if actual != {key: summary[key] for key in actual} or event_counts[0] != summary["event_count"]:
            raise ValueError("Funnel product publication differs from Gold summary")
        self.db.funnel_overview.replace_one({"_id": snapshot_id}, {
            **summary, **ownership, "_id": snapshot_id, "snapshot_id": snapshot_id}, upsert=True)
        self.verify(snapshot_id, summary)

    def verify(self, snapshot_id, expected):
        actual = self.overview(snapshot_id)
        if actual != expected:
            raise ValueError("Published MongoDB funnel overview differs from HDFS Gold")
        products = self.db.funnel_products.find({"snapshot_id": snapshot_id}, PROJECTION).sort("item_id", 1)
        fingerprint = product_fingerprint(products)
        if fingerprint != {key: expected[key] for key in fingerprint}:
            raise ValueError("Published MongoDB funnel products differ from HDFS Gold")

    def overview(self, snapshot_id):
        return self.db.funnel_overview.find_one({"_id": snapshot_id}, PROJECTION)

    def page(self, snapshot_id, *, is_final, item_id=None):
        summary = self.overview(snapshot_id)
        if summary is None:
            raise ValueError("Published funnel snapshot is missing")
        query = {"snapshot_id": snapshot_id}
        if item_id is not None:
            query["item_id"] = item_id
        products = list(self.db.funnel_products.find(query, PROJECTION)
                        .sort([("view_sessions", -1), ("item_id", 1)]).limit(20))
        return {"snapshot_id": snapshot_id, "is_final": is_final, "overview": summary,
                "products": products, "product_limit": 20}
