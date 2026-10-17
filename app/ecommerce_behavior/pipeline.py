"""Shared distributed analytics on checked Silver; never collect historical events."""
from contextlib import contextmanager

from pyspark import StorageLevel
from pyspark.sql import functions as F, Window

from ecommerce_analytics.snapshots import write_json
from .contracts import BEHAVIOR_VERSION, SESSION_GAP_MS, counts, product_fingerprint


@contextmanager
def tables(events, scope_id):
    cached = []

    def persist(frame):
        cached.append(frame.persist(StorageLevel.DISK_ONLY))
        return cached[-1]

    try:
        ordered = Window.partitionBy("visitor_id").orderBy("event_time", "event_id")
        cumulative = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        source = events.select("event_id", "visitor_id", "item_id", "event_type", "event_time")
        assigned = (source.withColumn("_ms", F.unix_millis("event_time"))
                    .withColumn("_previous", F.lag("_ms").over(ordered)))
        assigned = assigned.withColumn("_new", F.when(
            F.col("_previous").isNull() | ((F.col("_ms") - F.col("_previous")) > SESSION_GAP_MS), 1).otherwise(0))
        assigned = assigned.withColumn("_session", F.sum("_new").over(cumulative))
        session_window = (Window.partitionBy("visitor_id", "_session").orderBy("event_time", "event_id")
                          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
        assigned = persist(assigned.withColumn("_first", F.first("event_id").over(session_window))
            .withColumn("session_start_date", F.to_date(F.min("event_time").over(session_window)))
            .withColumn("session_id", F.sha2(F.to_json(F.array(
                F.lit(BEHAVIOR_VERSION), F.lit(scope_id), F.col("visitor_id"), F.col("_first"))), 256)))
        item_window = Window.partitionBy("session_id", "item_id")
        staged = assigned.withColumn("view_at", F.min(F.when(F.col("event_type") == "view", F.col("event_time"))).over(item_window))
        staged = staged.withColumn("cart_at", F.min(F.when((F.col("event_type") == "add_to_cart") &
            (F.col("event_time") > F.col("view_at")), F.col("event_time"))).over(item_window))
        staged = staged.withColumn("purchase_at", F.min(F.when((F.col("event_type") == "purchase") &
            (F.col("event_time") > F.col("cart_at")), F.col("event_time"))).over(item_window))
        items = persist(staged.groupBy("session_id", "visitor_id", "item_id", "session_start_date").agg(
            F.count("*").alias("event_count"), *[F.min(key).alias(key) for key in ("view_at", "cart_at", "purchase_at")]))
        flags = items.groupBy("session_id").agg(*[
            F.max(F.col(key + "_at").isNotNull().cast("long")).alias(key + "_reached")
            for key in ("view", "cart", "purchase")])
        sessions = persist(assigned.groupBy("session_id", "visitor_id", "session_start_date").agg(
            F.min("event_time").alias("session_start"), F.max("event_time").alias("session_end"),
            F.count("*").alias("event_count"), *[
                F.sum((F.col("event_type") == kind).cast("long")).alias(key)
                for key, kind in (("views", "view"), ("add_to_cart", "add_to_cart"), ("purchase_events", "purchase"))])
            .join(flags, "session_id"))
        products = items.groupBy("item_id").agg(F.sum("event_count").alias("event_count"),
            F.count("*").alias("total_sessions"), *[
                F.sum(F.col(key + "_at").isNotNull().cast("long")).alias(key + "_sessions")
                for key in ("view", "cart", "purchase")])
        for name, numerator, denominator in (
                ("view_to_cart_rate", F.col("cart_sessions"), "view_sessions"),
                ("cart_to_purchase_rate", F.col("purchase_sessions"), "cart_sessions"),
                ("view_to_purchase_rate", F.col("purchase_sessions"), "view_sessions"),
                ("view_drop_rate", F.col("view_sessions") - F.col("cart_sessions"), "view_sessions"),
                ("cart_drop_rate", F.col("cart_sessions") - F.col("purchase_sessions"), "cart_sessions")):
            products = products.withColumn(name, F.when(F.col(denominator) > 0, numerator / F.col(denominator)).otherwise(F.lit(0.0)))
        products = persist(products.withColumn("view_drop_count", F.col("view_sessions") - F.col("cart_sessions"))
                           .withColumn("cart_drop_count", F.col("cart_sessions") - F.col("purchase_sessions")))
        yield sessions, items, products
    finally:
        for frame in reversed(cached):
            frame.unpersist()


def product_rows(spark, root):
    for row in spark.read.parquet(root + "/funnel_products").orderBy("item_id").toLocalIterator():
        yield row.asDict()


def write_behavior(events, storage, root, scope_id):
    with tables(events, scope_id) as (sessions, items, products):
        row = sessions.agg(F.count("*").alias("total_sessions"), F.sum("event_count").alias("event_count"), *[
            F.sum(key + "_reached").alias(key + "_sessions") for key in ("view", "cart", "purchase")]).first().asDict()
        summary = {"behavior_version": BEHAVIOR_VERSION, "scope_id": scope_id,
                   **counts(**{key: int(value or 0) for key, value in row.items()}),
                   **product_fingerprint(row.asDict() for row in products.orderBy("item_id").toLocalIterator())}
        source_count = events.count()
        item_count = int(items.agg(F.sum("event_count")).first()[0] or 0)
        product_count = int(products.agg(F.sum("event_count")).first()[0] or 0)
        if {source_count, item_count, product_count, summary["event_count"]} != {source_count}:
            raise ValueError("Behavior event reconciliation failed")
        sessions.write.mode("errorifexists").partitionBy("session_start_date").parquet(root + "/sessions")
        items.write.mode("errorifexists").partitionBy("session_start_date").parquet(root + "/session_items")
        products.write.mode("errorifexists").parquet(root + "/funnel_products")
        write_json(storage, root + "/behavior.json", summary)
        quality = {"source_events": source_count, "session_events": summary["event_count"],
                   "session_item_events": item_count, "product_events": product_count, "passed": True}
        write_json(storage, root + "/behavior-quality.json", quality)
        return summary
