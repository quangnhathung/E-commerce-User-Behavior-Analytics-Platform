from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from helpers import BASE_MS, fixture
from ecommerce_analytics.contracts import (dataset_version, event_id, fingerprint_sources,
                                           normalize_record, normalize_partition)


class ContractTests(unittest.TestCase):
    def test_normalizes_all_events_and_keeps_string_ids(self):
        for raw, normalized in (("view", "view"), ("addtocart", "add_to_cart"), ("transaction", "purchase")):
            transaction = "00900" if raw == "transaction" else ""
            row = normalize_record(f"{BASE_MS},001,{raw},010,{transaction}", 2, "version", "worker")
            self.assertEqual([], row["error_codes"])
            self.assertEqual(normalized, row["event_type"])
            self.assertEqual("001", row["visitor_id"])
            self.assertEqual("010", row["item_id"])
            self.assertEqual(transaction or None, row["transaction_id"])
            self.assertIsNone(row["emitted_at"])

    def test_timestamp_is_integer_milliseconds_in_utc(self):
        row = normalize_record(f"{BASE_MS},1,view,10,", 2, "v", "w")
        self.assertEqual(datetime(2015, 5, 3, 3, 0, 4, 384000, tzinfo=timezone.utc), row["event_time"])
        self.assertEqual("2015-05-03", row["event_date"].isoformat())
        self.assertEqual(datetime(1970, 1, 1, tzinfo=timezone.utc),
                         normalize_record("0,0,view,0,", 2, "v", "w")["event_time"])

    def test_error_codes_retain_raw_line_and_source_position(self):
        cases = [(f"{BASE_MS},,view,1,", "MISSING_VISITOR_ID"),
                 (f"{BASE_MS},1,view,-1,", "INVALID_ITEM_ID"),
                 (f"{BASE_MS},1,other,1,", "UNKNOWN_EVENT_TYPE"),
                 (f"{BASE_MS},1,transaction,1,", "MISSING_TRANSACTION_ID"),
                 (f"{BASE_MS},1,transaction,1,no", "INVALID_TRANSACTION_ID"),
                 ("-1,1,view,1,", "INVALID_TIMESTAMP"),
                 ("253402300800000,1,view,1,", "INVALID_TIMESTAMP"),
                 (f"{BASE_MS},1,view,1", "FIELD_COUNT_MISMATCH"),
                 (f"{BASE_MS},1,view,1,,extra", "FIELD_COUNT_MISMATCH"),
                 ('"unterminated', "MALFORMED_CSV"), ("", "FIELD_COUNT_MISMATCH")]
        for line, error in cases:
            with self.subTest(line=line):
                row = normalize_record(line, 99, "v", "w")
                self.assertIn(error, row["error_codes"])
                self.assertEqual(line, row["raw_line"])
                self.assertEqual(99, row["source_row_number"])
                self.assertIsNone(row["event_id"])

    def test_only_first_header_is_skipped_and_multiple_errors_are_reported(self):
        header = "timestamp,visitorid,event,itemid,transactionid"
        rows = list(normalize_partition([(header, 0), (header, 1), ("bad,,other,-1,no", 2)], "v"))
        self.assertEqual(2, len(rows))
        self.assertEqual(2, rows[0]["source_row_number"])
        self.assertEqual(5, len(rows[1]["error_codes"]))

    def test_id_matches_canonical_contract_and_ignores_worker_or_row_number(self):
        fields = [str(BASE_MS), "1", "view", "10", ""]
        expected = hashlib.sha256(json.dumps([1, "retailrocket", "v", *fields],
                                             separators=(",", ":")).encode()).hexdigest()
        self.assertEqual(expected, event_id("v", fields))
        first = normalize_record(",".join(fields), 2, "v", "worker-1")
        second = normalize_record(",".join(fields), 100, "v", "worker-2")
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertNotEqual(expected, event_id("other", fields))
        self.assertNotEqual(expected, event_id("v", [str(BASE_MS), "1", "view", "11", ""]))

    def test_input_fingerprint_ignores_directory_and_mtime_but_tracks_all_files(self):
        import os
        import shutil
        with tempfile.TemporaryDirectory() as directory:
            first = fixture(Path(directory) / "first")
            second = Path(directory) / "second"
            shutil.copytree(first, second)
            original = fingerprint_sources(first)
            os.utime(second / "events.csv", (1, 1))
            self.assertEqual(original, fingerprint_sources(second))
            self.assertEqual(original["dataset_version"], dataset_version(list(reversed(original["source_files"]))))
            with (second / "category_tree.csv").open("a") as output:
                output.write("2,1\n")
            self.assertNotEqual(original["dataset_version"], fingerprint_sources(second)["dataset_version"])

    def test_missing_file_and_wrong_header_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            source = fixture(directory)
            (source / "events.csv").write_text("wrong,header\n")
            with self.assertRaisesRegex(ValueError, "Unexpected header"):
                fingerprint_sources(source)
            (source / "category_tree.csv").unlink()
            with self.assertRaises(FileNotFoundError):
                fingerprint_sources(source)


if __name__ == "__main__":
    unittest.main()
