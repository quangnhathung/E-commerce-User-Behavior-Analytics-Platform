"""Purchase registries expose complete immutable model/score snapshots only."""
from ecommerce_analytics.snapshots import utc_now
from ecommerce_ml.store import Store as MongoConnection
from .contracts import FEATURE_VERSION, digest, identifier, normalize, row_fingerprint, score_value, visitor
from .data import fire


class Store(MongoConnection):
    def prepare_record(self, collection, identity, fields, gold_exists):
        key = digest(identity)
        previous = collection.find_one({"_id": key})
        if previous is not None and (previous["identity"] != identity or any(previous.get(k) != v for k, v in fields.items())):
            raise ValueError("Purchase registry identity/path mismatch")
        if previous is not None and previous["status"] == "published" and not gold_exists:
            raise ValueError("Published purchase Gold is missing; restore its snapshot")
        collection.update_one({"_id": key}, {"$setOnInsert": {"identity": identity, **fields,
            "status": "prepared", "created_at": utc_now()}}, upsert=True)
        return key

    def prepare_model(self, training, gold_exists):
        if digest(training["identity"]) != identifier(training["model_version"]):
            raise ValueError("Purchase model registry ID mismatch")
        self.prepare_record(self.db.purchase_models, training["identity"], {
            "model_version": training["model_version"], "model_path": training["model_path"],
            "data_path": training["data"]["data_path"]}, gold_exists)

    def publish_model(self, training, publication, *, fault=None):
        self.prepare_model(training, True)
        key = training["model_version"]
        old = self.db.purchase_models.find_one({"_id": key})
        if old.get("report") is not None and old["report"] != publication:
            raise ValueError("Refusing to change a published purchase model report")
        fire(fault, "before_purchase_model_pointer")
        self.db.purchase_models.update_one({"_id": key}, {"$set": {
            "status": "published", "report": publication, "published_at": old.get("published_at", utc_now())}})
        fire(fault, "after_purchase_model_pointer")

    def model_status(self, version):
        identifier(version)
        value = self.db.purchase_models.find_one({"_id": version}, {"_id": 0})
        if value is None or value["status"] != "published":
            raise ValueError("Purchase model is not published")
        if digest(value["identity"]) != version:
            raise ValueError("Published purchase model identity changed")
        return value

    def prepare_scores(self, identity, path, gold_exists):
        key = digest(identity)
        self.db.purchase_scores.create_index([("score_run_id", 1), ("visitor_id", 1)], unique=True)
        self.prepare_record(self.db.purchase_score_runs, identity, {"score_run_id": key,
            "model_version": identity["model_version"], "score_path": path}, gold_exists)
        return key

    def verify_scores(self, run_id, report, threshold):
        record = self.db.purchase_score_runs.find_one({"_id": run_id})
        def rows():
            cursor = self.db.purchase_scores.find({"score_run_id": run_id}, {"_id": 0}).sort([("as_of", 1), ("visitor_id", 1)])
            for value in cursor:
                if value["model_version"] != record["model_version"] or value["threshold"] != threshold or value["feature_version"] != FEATURE_VERSION:
                    raise ValueError("Mongo purchase score model/threshold/version mismatch")
                yield value
        if row_fingerprint(rows(), scored=True, threshold=threshold) != report["fingerprint"]:
            raise ValueError("Mongo purchase score fingerprint/count differs from Gold")

    def publish_scores(self, identity, path, report, rows, *, fault=None):
        from pymongo import ReplaceOne
        key = self.prepare_scores(identity, path, True)
        previous = self.db.purchase_score_runs.find_one({"_id": key})
        if previous.get("report") is not None and previous["report"] != report:
            raise ValueError("Refusing to change published purchase scores")
        threshold = report["threshold"]
        if previous["status"] == "published":
            self.verify_scores(key, report, threshold)
            return
        operations = []
        for row in rows:
            document = {**normalize(row), "purchase_score": score_value(row["purchase_score"]),
                "predicted_label": row["predicted_label"], "threshold": threshold,
                "model_version": identity["model_version"], "feature_version": FEATURE_VERSION,
                "score_run_id": key, "_id": key + ":" + row["visitor_id"]}
            operations.append(ReplaceOne({"_id": document["_id"]}, document, upsert=True))
            if len(operations) == 100:
                self.db.purchase_scores.bulk_write(operations, ordered=True)
                operations = []
                fire(fault, "mongo_mid_purchase_scores")
        if operations:
            self.db.purchase_scores.bulk_write(operations, ordered=True)
        self.verify_scores(key, report, threshold)
        fire(fault, "before_purchase_score_pointer")
        self.db.purchase_score_runs.update_one({"_id": key}, {"$set": {
            "report": report, "status": "published", "published_at": utc_now()}})
        fire(fault, "after_purchase_score_pointer")

    def score_status(self, run_id, visitor_id=None):
        identifier(run_id)
        value = self.db.purchase_score_runs.find_one({"_id": run_id}, {"_id": 0})
        if value is None or value["status"] != "published":
            raise ValueError("Purchase score run is not published")
        if digest(value["identity"]) != run_id:
            raise ValueError("Published purchase score identity changed")
        if visitor_id is not None:
            visitor(visitor_id)
            value["visitor"] = self.db.purchase_scores.find_one({"_id": run_id + ":" + visitor_id}, {"_id": 0})
        return value
