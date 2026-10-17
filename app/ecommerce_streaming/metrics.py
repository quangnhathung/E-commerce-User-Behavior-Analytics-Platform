"""Exact Spark aggregates for a bounded run; never sum per-minute distinct counts."""
import hashlib

from pyspark.sql import functions as F
from ecommerce_analytics.contracts import canonical_json
from .contracts import iso


def aggregates():
    return [F.count("*").alias("processed_events"),
            *[F.sum(F.when(F.col("event_type") == event, 1).otherwise(0)).alias(name)
              for event, name in (("view", "views"), ("add_to_cart", "add_to_cart"), ("purchase", "purchase_events"))],
            F.countDistinct("transaction_id").alias("transactions"),
            F.countDistinct("visitor_id").alias("visitors"),
            F.countDistinct(F.when(F.col("event_type") == "purchase", F.col("visitor_id"))).alias("buyers")]


def clean_counts(row):
    result = {key: int(value or 0) for key, value in row.items()}
    result["buyer_rate"] = result["buyers"] / result["visitors"] if result["visitors"] else 0.0
    return result


def metrics(frame):
    from datetime import timedelta
    overview = clean_counts(frame.agg(*aggregates()).first().asDict())
    latest = frame.agg(F.max("event_time")).first()[0]
    active = frame.where((F.col("event_time") > F.lit(latest - timedelta(minutes=5))) &
                         (F.col("event_time") <= F.lit(latest))).select("visitor_id").distinct().count() if latest else 0
    overview.update(max_event_time=iso(latest) if latest else None, active_visitors=active)
    minutes = []
    grouped = frame.withColumn("window_start", F.date_trunc("minute", "event_time"))
    for row in grouped.groupBy("window_start").agg(*aggregates()).orderBy("window_start").toLocalIterator():
        values = row.asDict()
        start = iso(values.pop("window_start"))
        minutes.append({"window_start": start, **clean_counts(values)})
    identity = hashlib.sha256()
    for row in frame.select("event_id", "business_sha256").orderBy("event_id").toLocalIterator():
        identity.update((canonical_json([row.event_id, row.business_sha256]) + "\n").encode())
    return {"overview": overview, "minutes": minutes, "events_sha256": identity.hexdigest()}
