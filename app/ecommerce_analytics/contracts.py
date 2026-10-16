"""Versioned input contract and deterministic normalization; standard library only."""
import csv
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import socket

SCHEMA_VERSION = 1
TRANSFORM_VERSION = "events-v1"
SOURCE = "retailrocket"
HEADERS = {
    "category_tree.csv": ["categoryid", "parentid"],
    "events.csv": ["timestamp", "visitorid", "event", "itemid", "transactionid"],
    "item_properties_part1.csv": ["timestamp", "itemid", "property", "value"],
    "item_properties_part2.csv": ["timestamp", "itemid", "property", "value"],
}
EVENT_TYPES = {"view": "view", "addtocart": "add_to_cart", "transaction": "purchase"}
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MAX_TIMESTAMP_MS = 253402300799999  # 9999-12-31T23:59:59.999Z
SILVER_COLUMNS = (
    "schema_version", "dataset_version", "source", "run_id", "event_id",
    "visitor_id", "item_id", "event_type", "transaction_id", "event_time",
    "event_date", "emitted_at", "source_file", "source_row_number",
)


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def dataset_version(files):
    canonical = [{key: entry[key] for key in ("name", "size", "sha256")}
                 for entry in sorted(files, key=lambda entry: entry["name"])]
    return hashlib.sha256(canonical_json(canonical).encode("utf-8")).hexdigest()


def fingerprint_sources(directory):
    directory = Path(directory)
    files = []
    for name, header in sorted(HEADERS.items()):
        path = directory / name
        before = path.stat()
        if not path.is_file():
            raise ValueError(f"Expected a readable input file: {name}")
        with path.open(encoding="utf-8", newline="") as source:
            actual = next(csv.reader(source, strict=True), None)
        if actual != header:
            raise ValueError(f"Unexpected header in {name}: expected {header}, got {actual}")
        checksum = sha256_file(path)
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError(f"Source changed while hashing: {name}; retry with a stable input")
        files.append({"name": name, "size": after.st_size, "sha256": checksum})
    return {"dataset_version": dataset_version(files), "source_files": files}


def event_id(version, fields):
    payload = [SCHEMA_VERSION, SOURCE, version, *fields]
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def normalize_record(raw_line, source_row_number, version, worker_host):
    row = {name: None for name in SILVER_COLUMNS}
    row.update(schema_version=SCHEMA_VERSION, dataset_version=version, source=SOURCE,
               run_id="history-" + version, source_file="events.csv",
               source_row_number=source_row_number, raw_line=raw_line,
               error_codes=[], worker_host=worker_host)
    try:
        fields = next(csv.reader([raw_line], strict=True))
    except (csv.Error, StopIteration):
        row["error_codes"] = ["MALFORMED_CSV"]
        return row
    if len(fields) != 5:
        row["error_codes"] = ["FIELD_COUNT_MISMATCH"]
        return row
    timestamp, visitor, event, item, transaction = fields
    errors = row["error_codes"]
    row.update(visitor_id=visitor or None, item_id=item or None,
               event_type=EVENT_TYPES.get(event), transaction_id=transaction or None)
    if not re.fullmatch(r"[0-9]+", timestamp):
        errors.append("INVALID_TIMESTAMP")
    else:
        # Avoid both floating point time conversion and unbounded integer input.
        if len(timestamp) > 15 or int(timestamp) > MAX_TIMESTAMP_MS:
            errors.append("INVALID_TIMESTAMP")
        else:
            instant = EPOCH + timedelta(milliseconds=int(timestamp))
            row.update(event_time=instant, event_date=instant.date())
    for field, value in (("VISITOR_ID", visitor), ("ITEM_ID", item)):
        if not value:
            errors.append("MISSING_" + field)
        elif not re.fullmatch(r"[0-9]+", value):
            errors.append("INVALID_" + field)
    if event not in EVENT_TYPES:
        errors.append("UNKNOWN_EVENT_TYPE")
    if not transaction and event == "transaction":
        errors.append("MISSING_TRANSACTION_ID")
    elif transaction and not re.fullmatch(r"[0-9]+", transaction):
        errors.append("INVALID_TRANSACTION_ID")
    if not errors:
        row["event_id"] = event_id(version, fields)
    return row


def normalize_partition(records, version):
    host = socket.gethostname()
    for line, index in records:
        if index != 0:  # Only the first physical line is the header.
            yield normalize_record(line, index + 1, version, host)
