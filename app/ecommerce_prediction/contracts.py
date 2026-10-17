"""Versioned purchase contracts; portable oracles and selection rules."""
from datetime import timedelta
import hashlib
import json
import math
from itertools import islice

from ecommerce_ml.contracts import (FEATURES, COUNTS, FEATURE_VERSION, canonical, coverage, digest,
                                    identifier, instant, iso, normalize, reference, visitor, window)

DATA_VERSION = "purchase-data-v1"
MODEL_VERSION = "purchase-rf-v1"
SAMPLE_VERSION = "purchase-sampling-v1"
METRIC_VERSION = "purchase-metrics-v1"
SCORE_VERSION = "purchase-score-v1"
LABEL = "purchase_in_next_24h"
DEFAULT_SCORE_AS_OF = "2015-09-18T00:00:00.000Z"
BASELINE_COUNTS = {"train": {"rows": 5196014, "positives": 1750},
                   "validation": {"rows": 2560155, "positives": 729},
                   "test": {"rows": 2380550, "positives": 726}}
RF_PARAMS = {"seed": 42, "featureSubsetStrategy": "sqrt", "impurity": "gini", "maxBins": 32,
             "maxMemoryInMB": 64, "cacheNodeIds": False, "minInstancesPerNode": 1,
             "minInfoGain": 0.0, "subsamplingRate": 1.0, "bootstrap": True}
CANDIDATES = [{"numTrees": trees, "maxDepth": depth} for trees in (50, 100) for depth in (5, 8)]
THRESHOLDS = [index / 100 for index in range(101)]
AUDIT = ("visitor_id", "as_of", "last_event_time")


def default_schedule():
    result = []
    for split, first, last in (("train", "2015-05-11", "2015-07-15"),
            ("validation", "2015-07-17", "2015-08-15"), ("test", "2015-08-17", "2015-09-17")):
        current, end = instant(first + "T00:00:00.000Z"), instant(last + "T00:00:00.000Z")
        while current <= end:
            result.append({"split": split, "as_of": iso(current)})
            current += timedelta(days=1)
    return result


def validate_schedule(schedule):
    if not schedule:
        raise ValueError("Purchase schedule cannot be empty")
    seen, by_split = set(), {name: [] for name in ("train", "validation", "test")}
    for value in schedule:
        timestamp = instant(value["as_of"])
        if timestamp.hour or timestamp.minute or timestamp.second or timestamp.microsecond:
            raise ValueError("Training cutoffs must be at UTC midnight")
        if value["as_of"] in seen or value["split"] not in by_split:
            raise ValueError("Duplicate cutoff or invalid split")
        seen.add(value["as_of"])
        by_split[value["split"]].append(timestamp)
    if any(not values for values in by_split.values()):
        raise ValueError("Every temporal split needs at least one cutoff")
    if schedule != sorted(schedule, key=lambda value: value["as_of"]):
        raise ValueError("Schedule must be in chronological order")
    for left, right in (("train", "validation"), ("validation", "test")):
        if max(by_split[left]) + timedelta(days=2) > min(by_split[right]):
            raise ValueError("Temporal splits require the one-day purge gap")
    return schedule


def chunks(schedule):
    validate_schedule(schedule)
    result = []
    for name in ("train", "validation", "test"):
        values = [entry for entry in schedule if entry["split"] == name]
        result.extend(values[index:index + 7] for index in range(0, len(values), 7))
    return result


def labeled_reference(events, as_of):
    values = list(islice(events, 100_001))
    if len(values) > 100_000:
        raise ValueError("Reference input exceeds the small-fixture limit")
    start = instant(as_of)
    buyers = {row["visitor_id"] for row in values if row["event_type"] == "purchase" and
              row.get("transaction_id") is not None and start <= instant(iso(row["event_time"])) < start + timedelta(days=1)}
    return [{**row, LABEL: int(row["visitor_id"] in buyers)} for row in reference(values, as_of)]


def sample_key(dataset_version, as_of, visitor_id):
    return digest([SAMPLE_VERSION, dataset_version, as_of, visitor_id, "42"])


def sample_reference(rows, dataset_version):
    values = list(islice(rows, 100_001))
    if len(values) > 100_000:
        raise ValueError("Sampling oracle requires a bounded fixture")
    if any(type(row[LABEL]) is not int or row[LABEL] not in (0, 1) for row in values):
        raise ValueError("Purchase labels must be integer 0/1")
    positives = [row for row in values if row[LABEL] == 1]
    negatives = [row for row in values if row[LABEL] == 0]
    if not positives or not negatives:
        raise ValueError("Training requires both purchase classes")
    negatives.sort(key=lambda row: (sample_key(dataset_version, iso(row["as_of"]), row["visitor_id"]),
                                    iso(row["as_of"]), row["visitor_id"]))
    return sorted(positives + negatives[:20 * len(positives)], key=lambda row: (iso(row["as_of"]), row["visitor_id"]))


def score_value(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 <= value <= 1:
        raise ValueError("Purchase score must be finite and in [0,1]")
    return float(value)


def row_fingerprint(rows, *, labeled=False, scored=False, threshold=None):
    checksum, previous, count, positives, predicted = hashlib.sha256(), None, 0, 0, 0
    sums = {key: 0 for key in COUNTS}
    for row in rows:
        value = canonical(row)
        key = (value[1], value[0])
        if previous is not None and key <= previous:
            raise ValueError("Purchase rows require unique (AS_OF, visitor) in sorted order")
        previous = key
        if labeled:
            label = row[LABEL]
            if type(label) is not int or label not in (0, 1):
                raise ValueError("Purchase labels must be integer 0/1")
            value.append(label)
            positives += label
        if scored:
            score = score_value(row["purchase_score"])
            value.append(score.hex())  # Exact IEEE value; no decimal formatting ambiguity.
            if threshold is not None:
                label = row["predicted_label"]
                if type(label) is not int or label != int(score >= threshold):
                    raise ValueError("Predicted label differs from its selected threshold")
                predicted += label
                value.append(label)
        checksum.update((json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode())
        count += 1
        for name in COUNTS:
            sums[name] += row[name]
    return {"rows": count, "positives": positives if labeled else None,
            "predicted_positive": predicted if threshold is not None else None,
            "sums": sums, "sha256": checksum.hexdigest()}


def composite(days):
    ordered = sorted(days, key=lambda entry: entry["as_of"])
    if len({entry["as_of"] for entry in ordered}) != len(ordered):
        raise ValueError("Duplicate day in purchase fingerprint")
    return {"rows": sum(entry["fingerprint"]["rows"] for entry in ordered),
            "positives": sum(entry["fingerprint"]["positives"] or 0 for entry in ordered),
            "sha256": digest(ordered)}


def confusion(tp, fp, fn, tn):
    if any(type(value) is not int or value < 0 for value in (tp, fp, fn, tn)):
        raise ValueError("Confusion counts must be nonnegative integers")
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn, "precision": precision, "recall": recall,
            "f1": 2 * precision * recall / (precision + recall) if precision + recall else 0.0}


def choose_threshold(values):
    if [value["threshold"] for value in values] != THRESHOLDS:
        raise ValueError("Threshold report must cover the fixed 101-point grid")
    return max(values, key=lambda value: (value["f1"], value["precision"], value["threshold"]))


def choose_candidate(values):
    if len(values) != 4 or {digest(value["parameters"]) for value in values} != {digest(value) for value in CANDIDATES}:
        raise ValueError("All four RF candidates are required")
    if any(not math.isfinite(value["metrics"]["pr_auc"]) or not 0 <= value["metrics"]["pr_auc"] <= 1 for value in values):
        raise ValueError("Invalid candidate PR-AUC")
    best = max(value["metrics"]["pr_auc"] for value in values)
    return min((value for value in values if best - value["metrics"]["pr_auc"] <= 1e-12),
               key=lambda value: (value["parameters"]["numTrees"], value["parameters"]["maxDepth"]))
