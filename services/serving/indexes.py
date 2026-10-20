"""Explicit, idempotent serving index migration; never called by an HTTP request."""
import os


def apply(db):
    db.stream_runs.create_index([("config.pipeline_version", 1), ("config.base", 1),
                                ("created_at", -1), ("run_id", 1)], name="web_runs")
    db.stream_ml_predictions.create_index([("run_id", 1), ("batch_id", 1),
        ("purchase_score", -1), ("visitor_id", 1)], name="web_prediction_rank")
    db.kpi_minute.create_index([("run_id", 1), ("batch_id", 1), ("window_start", 1)], name="web_minutes")
    print("Serving indexes verified; no snapshot content changed.")


if __name__ == "__main__":
    from pymongo import MongoClient
    with MongoClient(os.getenv("MONGO_HOST", "127.0.0.1"), port=int(os.getenv("MONGO_PORT", "27018")),
                     username=os.environ["MONGO_USERNAME"], password=os.environ["MONGO_PASSWORD"],
                     authSource=os.environ["MONGO_DATABASE"], serverSelectionTimeoutMS=5000) as client:
        apply(client[os.environ["MONGO_DATABASE"]])
