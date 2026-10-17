"""Deterministic Mongo upserts; a final registry write exposes complete models."""
import os

from ecommerce_analytics.snapshots import utc_now
from .contracts import digest, fingerprint, identifier, normalize, visitor


class Store:
    def __init__(self, database=None):
        self.client = None
        if database is None:
            from pymongo import MongoClient
            self.client = MongoClient("mongodb", username=os.environ["MONGO_USERNAME"],
                password=os.environ["MONGO_PASSWORD"], authSource=os.environ["MONGO_DATABASE"],
                serverSelectionTimeoutMS=5000, socketTimeoutMS=10000, connectTimeoutMS=5000)
            database = self.client[os.environ["MONGO_DATABASE"]]
        self.db = database

    def close(self):
        if self.client is not None:
            self.client.close()

    def prepare(self, version, identity, path, gold_exists):
        identifier(version)
        if digest(identity) != version:
            raise ValueError("Model registry ID differs from identity")
        previous = self.db.segment_models.find_one({"_id": version})
        if previous is not None and (previous["identity"] != identity or previous["model_path"] != path):
            raise ValueError("Model registry identity mismatch")
        if previous is not None and previous["status"] == "published" and not gold_exists:
            raise ValueError("Published model Gold is missing; restore its snapshot")
        self.db.segment_models.update_one({"_id": version}, {"$setOnInsert": {
            "model_version": version, "identity": identity, "model_path": path,
            "status": "prepared", "created_at": utc_now()}}, upsert=True)
        self.db.visitor_segments.create_index([("model_version", 1), ("visitor_id", 1)], unique=True)
        self.db.segment_profiles.create_index([("model_version", 1), ("cluster_id", 1)], unique=True)

    def verify(self, version, report):
        projection = {"_id": 0, "model_version": 0}
        actual = fingerprint(self.db.visitor_segments.find({"model_version": version}, projection).sort("visitor_id", 1), assignments=True)
        if actual != report["assignment_fingerprint"]:
            raise ValueError("Mongo assignment count/fingerprint mismatch")
        profiles = list(self.db.segment_profiles.find({"model_version": version}, projection).sort("cluster_id", 1))
        if profiles != report["profiles"] or sum(value["count"] for value in profiles) != actual["rows"]:
            raise ValueError("Mongo profile coverage mismatch")

    def publish(self, version, identity, path, report, rows, *, fault=None):
        from pymongo import ReplaceOne
        self.prepare(version, identity, path, True)
        previous = self.db.segment_models.find_one({"_id": version})
        if previous.get("report") is not None and previous["report"] != report:
            raise ValueError("Refusing to change a published model report")
        # Published versions are immutable. Verify existing documents rather than repair in place.
        if previous["status"] == "published":
            self.verify(version, report)
            return
        operations = []
        for row in rows:
            value = {**normalize(row), "cluster_id": int(row["cluster_id"])}
            key = version + ":" + value["visitor_id"]
            operations.append(ReplaceOne({"_id": key}, {"_id": key, "model_version": version, **value}, upsert=True))
            if len(operations) == 100:
                self.db.visitor_segments.bulk_write(operations, ordered=True)
                operations = []
                if fault:
                    fault("mongo_mid_assignments")
        if operations:
            self.db.visitor_segments.bulk_write(operations, ordered=True)
        for profile in report["profiles"]:
            key = version + ":" + str(profile["cluster_id"])
            self.db.segment_profiles.replace_one({"_id": key}, {"_id": key, "model_version": version, **profile}, upsert=True)
        self.verify(version, report)
        if fault:
            fault("before_model_pointer")
        self.db.segment_models.update_one({"_id": version}, {"$set": {
            "report": report, "status": "published", "published_at": utc_now()}})
        if fault:
            fault("after_model_pointer")

    def status(self, version, visitor_id=None):
        identifier(version)
        record = self.db.segment_models.find_one({"_id": version}, {"_id": 0})
        if record is None or record["status"] != "published":
            raise ValueError("Segment model is not published")
        result = dict(record)
        if visitor_id is not None:
            visitor(visitor_id)
            result["visitor"] = self.db.visitor_segments.find_one({"_id": version + ":" + visitor_id}, {"_id": 0})
        return result
