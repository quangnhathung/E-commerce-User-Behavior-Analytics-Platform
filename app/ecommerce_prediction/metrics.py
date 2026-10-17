"""Exact Spark ranking curves, fixed threshold grid and daily top-one-percent."""
import math
from itertools import islice

from ecommerce_ml.native import NativeML
from .contracts import LABEL, THRESHOLDS, confusion, iso, score_value


def validate_scores(frame, *, labeled=True):
    from pyspark.sql import functions as F
    bad = F.col("purchase_score").isNull() | F.isnan("purchase_score") | ~F.col("purchase_score").between(0.0, 1.0)
    if labeled:
        bad = bad | F.col(LABEL).isNull() | ~F.col(LABEL).isin(0, 1)
    if frame.where(bad).limit(1).count():
        raise ValueError("Invalid purchase score/label")


def threshold_metrics(frame, thresholds=None):
    from pyspark.sql import functions as F
    thresholds = THRESHOLDS if thresholds is None else thresholds
    expressions = [F.count("*").alias("n"), F.sum(LABEL).alias("p")]
    for index, threshold in enumerate(thresholds):
        condition = F.col("purchase_score") >= F.lit(threshold)
        expressions.extend((F.sum(condition.cast("long")).alias("pred" + str(index)),
            F.sum((condition & (F.col(LABEL) == 1)).cast("long")).alias("tp" + str(index))))
    result = frame.agg(*expressions).first().asDict()
    n, p = int(result["n"]), int(result["p"] or 0)
    if not n:
        raise ValueError("Cannot evaluate an empty purchase cohort")
    values = []
    for index, threshold in enumerate(thresholds):
        tp, predicted = int(result["tp" + str(index)] or 0), int(result["pred" + str(index)] or 0)
        fp = predicted - tp
        values.append({"threshold": threshold, **confusion(tp, fp, p - tp, n - p - fp)})
    return values


def auc(spark, frame):
    from pyspark.sql import functions as F
    counts = frame.agg(F.count("*").alias("n"), F.sum(LABEL).alias("p")).first()
    if not counts[0] or not 0 < (counts[1] or 0) < counts[0]:
        raise ValueError("AUC requires both classes in the evaluated split")
    evaluator = (NativeML(spark).ml.evaluation.BinaryClassificationEvaluator()
                 .setLabelCol(LABEL).setRawPredictionCol("purchase_score").setNumBins(0))
    metrics = evaluator.getMetrics(frame._jdf)
    try:
        values = {"pr_auc": float(metrics.areaUnderPR()), "roc_auc": float(metrics.areaUnderROC())}
    finally:
        metrics.unpersist()
    if any(not math.isfinite(value) or not 0 <= value <= 1 for value in values.values()):
        raise ValueError("Nonfinite or invalid purchase AUC")
    return values


def top_percent(frame):
    from pyspark.sql import functions as F, Window
    counts = frame.groupBy("as_of").agg(F.count("*").alias("cohort"))
    ranked = frame.join(counts, "as_of").withColumn("rank", F.row_number().over(
        Window.partitionBy("as_of").orderBy(F.col("purchase_score").desc(), F.col("visitor_id").asc())))
    selected = ranked.where(F.col("rank") <= F.ceil(F.col("cohort") / F.lit(100.0)))
    days = [{"as_of": iso(row["as_of"]), "selected": int(row["selected"]), "positives": int(row["positives"])}
            for row in selected.groupBy("as_of").agg(F.count("*").alias("selected"),
                F.sum(LABEL).alias("positives")).orderBy("as_of").collect()]
    n, p = sum(row["selected"] for row in days), sum(row["positives"] for row in days)
    return {"precision_at_1pct": p / n if n else 0.0, "top_1pct": {"selected": n, "positives": p, "days": days}}


def evaluate(spark, frame, threshold):
    validate_scores(frame)
    return {**auc(spark, frame), **threshold_metrics(frame, [threshold])[0], **top_percent(frame)}


def baselines(spark, frame, prevalence):
    from pyspark.sql import functions as F
    result = {}
    for name, score, threshold in (("always_no_purchase", 0.0, 1.0), ("train_prevalence", prevalence, 0.5)):
        constant = frame.withColumn("purchase_score", F.lit(score))
        result[name] = {"score": score, **evaluate(spark, constant, threshold)}
    return result


def reference_metrics(rows, threshold):
    """Bounded fixture oracle matching Spark's trapezoidal PR/ROC conventions."""
    values = list(islice(rows, 100_001))
    if not values or len(values) > 100_000:
        raise ValueError("Metric oracle requires a bounded nonempty fixture")
    if any(type(row[LABEL]) is not int or row[LABEL] not in (0, 1) for row in values):
        raise ValueError("Purchase labels must be integer 0/1")
    p = sum(row[LABEL] for row in values)
    n = len(values) - p
    if not p or not n:
        raise ValueError("Metric oracle requires both classes")
    groups, dates = {}, {}
    tp = fp = 0
    for row in values:
        score = score_value(row["purchase_score"])
        counts = groups.setdefault(score, [0, 0])
        counts[0 if row[LABEL] else 1] += 1
        dates.setdefault(iso(row["as_of"]), []).append(row)
        tp += int(score >= threshold and row[LABEL] == 1)
        fp += int(score >= threshold and row[LABEL] == 0)
    pr, roc, cp, cn = [], [(0.0, 0.0)], 0, 0
    for score in sorted(groups, reverse=True):
        gp, gn = groups[score]
        cp += gp
        cn += gn
        pr.append((cp / p, cp / (cp + cn)))
        roc.append((cn / n, cp / p))
    pr.insert(0, (0.0, pr[0][1]))
    roc.append((1.0, 1.0))
    area = lambda curve: sum((b[0] - a[0]) * (a[1] + b[1]) / 2 for a, b in zip(curve, curve[1:]))
    days = []
    for date, cohort in sorted(dates.items()):
        cohort.sort(key=lambda row: (-row["purchase_score"], row["visitor_id"]))
        top = cohort[:math.ceil(len(cohort) / 100)]
        days.append({"as_of": date, "selected": len(top), "positives": sum(row[LABEL] for row in top)})
    total, positive = sum(row["selected"] for row in days), sum(row["positives"] for row in days)
    return {"pr_auc": area(pr), "roc_auc": area(roc), "threshold": threshold,
            **confusion(tp, fp, p - tp, n - fp), "precision_at_1pct": positive / total,
            "top_1pct": {"selected": total, "positives": positive, "days": days}}
