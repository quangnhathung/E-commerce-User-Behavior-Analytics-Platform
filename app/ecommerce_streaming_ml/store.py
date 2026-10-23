"""ML rows remain invisible until the existing atomic KPI/funnel run pointer commits."""
from ecommerce_streaming.store import Store as StreamingStore
from ecommerce_streaming.contracts import digest
from ecommerce_ml.contracts import visitor
from .contracts import normalized_prediction, prediction_fingerprint


class Store(StreamingStore):
    def ensure_run(self, config):
        super().ensure_run(config)
        self.db.stream_ml_predictions.create_index([("run_id", 1), ("batch_id", 1), ("visitor_id", 1)], unique=True)

    def verify_ml(self, config, batch_id, quality):
        key = f"{config['run_id']}:{batch_id}"
        registry = self.db.stream_ml_snapshots.find_one({"_id": key})
        if registry is None or registry["quality"] != quality or registry["config_sha256"] != digest(config):
            raise ValueError("Streaming ML registry differs from Gold")
        def values():
            for value in self.db.stream_ml_predictions.find({"run_id": config["run_id"], "batch_id": batch_id}, {"_id": 0}).sort("visitor_id", 1):
                if value.get("run_id") != config["run_id"] or value.get("batch_id") != batch_id:
                    raise ValueError("Prediction source coordinates changed")
                yield value
        actual = prediction_fingerprint(values(), config["bindings"], quality["as_of"], quality["scored_at"])
        if actual != quality["fingerprint"]:
            raise ValueError("Mongo streaming ML fingerprint/count differs from Gold")

    def publish_ml(self, config, batch_id, quality, rows, fault=None):
        from pymongo import ReplaceOne
        run_id, key = config["run_id"], f"{config['run_id']}:{batch_id}"
        previous = self.db.stream_ml_snapshots.find_one({"_id": key})
        if previous is not None and (previous["quality"] != quality or previous["config_sha256"] != digest(config)):
            raise ValueError("Refusing to change an immutable streaming ML snapshot")
        if previous is not None and previous["status"] == "published":
            self.verify_ml(config, batch_id, quality)
            return
        self.db.stream_ml_snapshots.update_one({"_id": key}, {"$setOnInsert": {
            "run_id": run_id, "batch_id": batch_id, "config_sha256": digest(config),
            "quality": quality, "status": "prepared"}}, upsert=True)
        operations = []
        for row in rows:
            value = normalized_prediction(row, config["bindings"], quality["as_of"], quality["scored_at"])
            identity = key + ":" + value["visitor_id"]
            operations.append(ReplaceOne({"_id": identity}, {"_id": identity, "run_id": run_id, "batch_id": batch_id, **value}, upsert=True))
            if len(operations) == 100:
                self.db.stream_ml_predictions.bulk_write(operations, ordered=True)
                operations = []
                if fault: fault("mongo_mid_ml")
        if operations:
            self.db.stream_ml_predictions.bulk_write(operations, ordered=True)
        self.verify_ml(config, batch_id, quality)
        self.db.stream_ml_snapshots.update_one({"_id": key}, {"$set": {"status": "published"}})

    def publish(self, config, batch_id, metrics, metadata, fault=None, *, behavior=None, products=None, ml=None, predictions=None):
        if behavior is None or ml is None or predictions is None:
            raise ValueError("Streaming ML needs complete KPI, funnel and prediction snapshots")
        if batch_id < self.load(config)["committed_batch_id"]:
            raise ValueError("Refusing to move the run snapshot pointer backwards")
        if ml["fingerprint"]["rows"] != metrics["overview"]["visitors"]:
            raise ValueError("Streaming ML predictions must cover every accepted visitor")
        self.publish_ml(config, batch_id, ml, predictions, fault)
        return super().publish(config, batch_id, metrics, {**metadata, "ml": ml}, fault=fault,
                               behavior=behavior, products=products)

    def status(self, config, visitor_id=None):
        result = super().status(config)
        batch_id = result["committed_batch_id"]
        result["demo_source"], result["bindings"] = config["demo_source"], config["bindings"]
        if batch_id < 0:
            failed = result["consumer"]["status"] in ("failed", "interrupted")
            result["scoring"] = {"status": "not_ready" if failed else "waiting_for_events",
                "as_of": None, "fingerprint": None, "reason": result["consumer"].get("reason")}
        else:
            if result["batch"] is None:
                raise ValueError("Committed streaming ML batch metadata is missing")
            quality = result["batch"].get("ml")
            if quality is None or result["funnel"]["overview"] is None:
                raise ValueError("Committed streaming ML/funnel snapshot is missing")
            key = f"{config['run_id']}:{batch_id}"
            registry = self.db.stream_ml_snapshots.find_one({"_id": key})
            if registry is None or registry["status"] != "published" or registry["quality"] != quality or registry["config_sha256"] != digest(config):
                raise ValueError("Committed streaming ML registry is incomplete")
            result["scoring"] = {"snapshot_id": key, **quality}
        processed = result["batch"]["processed_offsets"] if result["batch"] else config["initial_offsets"]
        result["backlog"] = sum(max(0, offset - processed.get(partition, 0))
                                for partition, offset in result["producer"]["end_offsets"].items())
        if visitor_id is not None:
            visitor(visitor_id)
            result["visitor"] = (self.db.stream_ml_predictions.find_one(
                {"_id": f"{config['run_id']}:{batch_id}:{visitor_id}"}, {"_id": 0}) if batch_id >= 0 else None)
            if result["visitor"] is not None:
                normalized_prediction(result["visitor"], config["bindings"], quality["as_of"], quality["scored_at"])
        return result

    def cleanup(self, run_ids):
        super().cleanup(run_ids)
        for name in ("stream_ml_snapshots", "stream_ml_predictions"):
            self.db[name].delete_many({"run_id": {"$in": list(run_ids)}})
