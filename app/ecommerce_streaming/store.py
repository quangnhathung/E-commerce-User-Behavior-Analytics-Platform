"""Mongo control plane and immutable KPI snapshots with an atomic run pointer."""
import os

from .contracts import digest, utc_now


class Store:
    def __init__(self, database=None):
        self.client = None
        if database is None:
            from pymongo import MongoClient
            self.client = MongoClient("mongodb", username=os.environ["MONGO_USERNAME"],
                                      password=os.environ["MONGO_PASSWORD"],
                                      authSource=os.environ["MONGO_DATABASE"],
                                      serverSelectionTimeoutMS=5000, socketTimeoutMS=10000,
                                      connectTimeoutMS=5000)
            database = self.client[os.environ["MONGO_DATABASE"]]
        self.db = database

    def close(self):
        if self.client is not None:
            self.client.close()

    def ensure_run(self, config):
        run_id = config["run_id"]
        self.db.stream_runs.update_one({"_id": run_id}, {"$setOnInsert": {
            "run_id": run_id, "config": config, "config_sha256": digest(config),
            "created_at": utc_now(), "committed_batch_id": -1, "worker_hosts": [],
            "producer": {"status": "pending", "last_seq": -1, "end_offsets": config["initial_offsets"]},
            "consumer": {"status": "prepared"}}}, upsert=True)
        self.load(config)
        for name in ("stream_batches", "kpi_overview", "kpi_minute"):
            self.db[name].create_index([("run_id", 1), ("batch_id", 1)])

    def load(self, config):
        document = self.db.stream_runs.find_one({"_id": config["run_id"]})
        if document is None or document.get("config_sha256") != digest(config) or document.get("config") != config:
            raise ValueError("Run registry missing or differs from the immutable prepared config")
        return document

    def producer_progress(self, config, cursor, status):
        previous = self.load(config)["producer"]
        now = utc_now()
        self.db.stream_runs.update_one({"_id": config["run_id"]}, {"$set": {
            "producer": {**cursor, "status": status, "updated_at": now,
                         "started_at": previous.get("started_at", now),
                         "completed_at": now if status == "completed" else None}}})

    def consumer_status(self, config, status, **extra):
        self.load(config)
        self.db.stream_runs.update_one({"_id": config["run_id"]}, {"$set": {
            "consumer.status": status, "consumer.updated_at": utc_now(),
            **{"consumer." + key: value for key, value in extra.items()}}})

    def publish(self, config, batch_id, metrics, metadata, fault=None):
        from pymongo import ReplaceOne
        run_id = config["run_id"]
        key = f"{run_id}:{batch_id}"
        self.db.kpi_overview.replace_one({"_id": key}, {
            "_id": key, "run_id": run_id, "batch_id": batch_id, **metrics["overview"]}, upsert=True)
        operations = []
        for index, minute in enumerate(metrics["minutes"]):
            identity = key + ":" + minute["window_start"]
            operations.append(ReplaceOne({"_id": identity}, {
                "_id": identity, "run_id": run_id, "batch_id": batch_id, **minute}, upsert=True))
            if len(operations) == 100 or index == 0:
                self.db.kpi_minute.bulk_write(operations, ordered=True)
                operations = []
                if fault:
                    fault("mongo_mid_minutes")
        if operations:
            self.db.kpi_minute.bulk_write(operations, ordered=True)
        if fault:
            fault("mongo_before_pointer")
        self.db.stream_batches.replace_one({"_id": key}, {
            "_id": key, "run_id": run_id, "batch_id": batch_id, **metadata,
            "published_at": utc_now()}, upsert=True)
        # A reader pins this pointer before fetching overview/minute documents.
        result = self.db.stream_runs.update_one({"_id": run_id, "committed_batch_id": {"$lte": batch_id}}, {
            "$set": {"committed_batch_id": batch_id, "consumer.status": "running", "updated_at": utc_now()},
            "$addToSet": {"worker_hosts": {"$each": sorted(metadata["worker_rows"])}}})
        if result.matched_count != 1:
            raise ValueError("Refusing to move the run snapshot pointer backwards")
        if fault:
            fault("after_mongo_publish")
        return utc_now()

    def publication_timing(self, config, batch_id, publication_at, emitted_times):
        from .contracts import instant
        latency = [max(0.0, (instant(publication_at) - instant(value)).total_seconds()) for value in emitted_times]
        self.db.stream_batches.update_one({"_id": f"{config['run_id']}:{batch_id}"}, {"$set": {
            "publication_at": publication_at, "latency_seconds": latency}})

    def status(self, config):
        run = self.load(config)
        batch_id = run["committed_batch_id"]
        key = f"{config['run_id']}:{batch_id}"
        projection = {"_id": 0, "run_id": 0, "batch_id": 0}
        overview = self.db.kpi_overview.find_one({"_id": key}, projection) if batch_id >= 0 else None
        batch = self.db.stream_batches.find_one({"_id": key}, {"_id": 0}) if batch_id >= 0 else None
        return {"run_id": config["run_id"], "producer": run["producer"], "consumer": run["consumer"],
                "committed_batch_id": batch_id, "worker_hosts": run["worker_hosts"],
                "overview": overview, "batch": batch, "report": run.get("report")}

    def finish(self, config, report):
        self.db.stream_runs.update_one({"_id": config["run_id"]}, {"$set": {
            "consumer.status": "completed", "consumer.updated_at": utc_now(), "report": report}})

    def cleanup(self, run_ids):
        for name in ("stream_runs", "stream_batches", "kpi_overview", "kpi_minute"):
            self.db[name].delete_many({"run_id": {"$in": list(run_ids)}})
