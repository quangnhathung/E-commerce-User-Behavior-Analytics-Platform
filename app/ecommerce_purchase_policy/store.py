"""Separate policy registry, with versioned scores readable by the existing CLI."""
from ecommerce_analytics.snapshots import utc_now
from ecommerce_prediction.contracts import (FEATURE_VERSION, digest, identifier, normalize,
                                           row_fingerprint, score_value)
from ecommerce_prediction.data import fire
from ecommerce_prediction.store import Store as PurchaseStore
from .contracts import SCORE_CONTRACT


class Store(PurchaseStore):
    def prepare_policy(self, identity, selection_path, evaluation_path, gold_exists):
        key = digest(identity)
        self.prepare_record(self.db.purchase_policies, identity, {"policy_version": key,
            "model_version": identity["model_version"], "selection_path": selection_path,
            "evaluation_path": evaluation_path}, gold_exists)
        return key

    def publish_policy(self, identity, selection_path, evaluation_path, report, *, fault=None):
        key = self.prepare_policy(identity, selection_path, evaluation_path, True)
        previous = self.db.purchase_policies.find_one({"_id": key})
        if previous.get("report") is not None and previous["report"] != report:
            raise ValueError("Refusing to change a published policy report")
        fire(fault, "before_policy_pointer")
        self.db.purchase_policies.update_one({"_id": key}, {"$set": {"status": "published",
            "report": report, "published_at": previous.get("published_at", utc_now())}})
        fire(fault, "after_policy_pointer")

    def policy_status(self, key):
        identifier(key)
        value = self.db.purchase_policies.find_one({"_id": key}, {"_id": 0})
        if value is None or value["status"] != "published":
            raise ValueError("Purchase policy is not published")
        if digest(value["identity"]) != key:
            raise ValueError("Published purchase policy identity changed")
        return value

    def prepare_policy_scores(self, identity, path, gold_exists):
        if identity.get("score_version") != SCORE_CONTRACT:
            raise ValueError("Unexpected policy score contract")
        key = digest(identity)
        self.db.purchase_scores.create_index([("score_run_id", 1), ("visitor_id", 1)], unique=True)
        self.prepare_record(self.db.purchase_score_runs, identity, {"score_run_id": key,
            "model_version": identity["model_version"], "score_path": path,
            "policy_version": identity["policy_version"], "source_score_run_id": identity["source_score_run_id"]}, gold_exists)
        return key

    def verify_policy_scores(self, identity, report):
        key, threshold = digest(identity), identity["threshold"]
        def rows():
            for value in self.db.purchase_scores.find({"score_run_id": key}, {"_id": 0}).sort([("as_of", 1), ("visitor_id", 1)]):
                expected = {"model_version": identity["model_version"], "policy_version": identity["policy_version"],
                    "source_score_run_id": identity["source_score_run_id"], "threshold": threshold,
                    "feature_version": FEATURE_VERSION}
                if any(value.get(name) != wanted for name, wanted in expected.items()):
                    raise ValueError("Policy score Mongo metadata differs from Gold")
                yield value
        if row_fingerprint(rows(), scored=True, threshold=threshold) != report["fingerprint"]:
            raise ValueError("Policy score Mongo fingerprint/count differs from Gold")

    def publish_policy_scores(self, identity, path, report, rows, *, fault=None):
        from pymongo import ReplaceOne
        key = self.prepare_policy_scores(identity, path, True)
        previous = self.db.purchase_score_runs.find_one({"_id": key})
        if previous.get("report") is not None and previous["report"] != report:
            raise ValueError("Refusing to change a published policy score report")
        if previous["status"] == "published":
            self.verify_policy_scores(identity, report)
            return
        operations = []
        for row in rows:
            document = {**normalize(row), "purchase_score": score_value(row["purchase_score"]),
                "predicted_label": row["predicted_label"], "threshold": identity["threshold"],
                "model_version": identity["model_version"], "policy_version": identity["policy_version"],
                "source_score_run_id": identity["source_score_run_id"], "feature_version": FEATURE_VERSION,
                "score_run_id": key, "_id": key + ":" + row["visitor_id"]}
            operations.append(ReplaceOne({"_id": document["_id"]}, document, upsert=True))
            if len(operations) == 100:
                self.db.purchase_scores.bulk_write(operations, ordered=True)
                operations = []
                fire(fault, "mongo_mid_policy_scores")
        if operations:
            self.db.purchase_scores.bulk_write(operations, ordered=True)
        self.verify_policy_scores(identity, report)
        fire(fault, "before_policy_score_pointer")
        self.db.purchase_score_runs.update_one({"_id": key}, {"$set": {"status": "published", "report": report,
            "published_at": utc_now()}})
        fire(fault, "after_policy_score_pointer")
