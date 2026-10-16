"""Real Spark/HDFS application checks with disposable, generated data."""
import argparse
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest import mock
import uuid

from pyspark.sql import SparkSession
from ecommerce_analytics.contracts import TRANSFORM_VERSION, event_id, fingerprint_sources, sha256_file
from ecommerce_analytics.hdfs import Hdfs
from ecommerce_analytics.ingestion import run_ingest
from ecommerce_analytics.snapshots import write_json

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from helpers import BASE_MS, fixture


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--code-archive", required=True)
    parser.add_argument("--code-sha256", required=True)
    args = parser.parse_args()
    if sha256_file(args.code_archive) != args.code_sha256:
        raise ValueError("Test application archive checksum mismatch")
    spark = SparkSession.builder.appName("retailrocket-data-tests").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    storage = Hdfs(spark)
    test_root = "/ecommerce/_tests/ingest/" + uuid.uuid4().hex
    storage.mkdir(test_root)
    temporary = tempfile.TemporaryDirectory(prefix="ecommerce-data-tests-")
    source = fixture(Path(temporary.name) / "source")
    code_sha = args.code_sha256
    shared = None
    result = None
    try:
        shared = run_ingest(spark, source, code_sha, base=test_root + "/baseline")

        class DataTests(unittest.TestCase):
            def test_quality_and_actual_distributed_execution(self):
                quality = shared["quality"]
                for name, value in {"source_rows": 143, "silver_rows": 132, "duplicate_rows": 1,
                                    "invalid_rows": 10, "distinct_visitors": 129, "distinct_items": 130,
                                    "distinct_transactions": 1, "distinct_buyers": 1}.items():
                    self.assertEqual(value, quality[name], name)
                self.assertEqual("warning", quality["quality_status"])
                self.assertEqual({"view": 129, "add_to_cart": 1, "purchase": 2}, quality["silver_event_counts"])
                self.assertEqual({"spark-worker-1", "spark-worker-2"}, set(quality["worker_rows"]))
                self.assertGreaterEqual(shared["manifest"]["minimum_block_replicas"], 2)
                data = spark.read.parquet(shared["silver_path"] + "/events")
                key = event_id(shared["dataset_version"], [str(BASE_MS), "1", "view", "10", ""])
                row = data.where(data.event_id == key).first()
                self.assertEqual(2, row.source_row_number)
                self.assertEqual("2015-05-03", row.event_date.isoformat())
                self.assertEqual(384000, row.event_time.microsecond)
                self.assertIsNone(row.emitted_at)
                quarantine = spark.read.parquet(shared["silver_path"] + "/quarantine")
                self.assertEqual(10, quarantine.count())
                self.assertTrue(all(row.error_codes for row in quarantine.select("error_codes").collect()))

            def test_reuse_does_not_rewrite_committed_manifest(self):
                before = storage.sha256(shared["silver_path"] + "/manifest.json")
                again = run_ingest(spark, source, code_sha, base=test_root + "/baseline")
                self.assertTrue(again["bronze_reused"])
                self.assertTrue(again["silver_reused"])
                self.assertEqual(shared["quality"], again["quality"])
                self.assertEqual(before, storage.sha256(shared["silver_path"] + "/manifest.json"))

            def test_retry_before_silver_commit(self):
                base = test_root + "/before-commit"
                guard = base + "/unrelated"
                storage.mkdir(base)
                storage.write_bytes(guard, b"keep")

                def fault(stage):
                    if stage == "before_silver_publish":
                        raise RuntimeError("injected before commit")

                with self.assertRaisesRegex(RuntimeError, "injected"):
                    run_ingest(spark, source, code_sha, base=base, fault=fault)
                final = base + "/silver/retailrocket/" + shared["dataset_version"] + "/" + TRANSFORM_VERSION
                self.assertFalse(storage.exists(final))
                again = run_ingest(spark, source, code_sha, base=base)
                self.assertTrue(again["bronze_reused"])
                self.assertFalse(again["silver_reused"])
                self.assertEqual(132, again["quality"]["silver_rows"])
                self.assertTrue(storage.exists(guard))

            def test_retry_after_silver_commit(self):
                base = test_root + "/after-commit"

                def fault(stage):
                    if stage == "after_silver_publish":
                        raise RuntimeError("injected after commit")

                with self.assertRaisesRegex(RuntimeError, "injected"):
                    run_ingest(spark, source, code_sha, base=base, fault=fault)
                again = run_ingest(spark, source, code_sha, base=base)
                self.assertTrue(again["bronze_reused"] and again["silver_reused"])
                self.assertEqual(132, again["quality"]["silver_rows"])

            def test_changed_input_gets_new_version(self):
                changed = Path(temporary.name) / "changed"
                shutil.copytree(source, changed)
                with (changed / "events.csv").open("a") as output:
                    output.write(f"{BASE_MS+9999},999,view,999,\n")
                again = run_ingest(spark, changed, code_sha, base=test_root + "/baseline")
                self.assertNotEqual(shared["dataset_version"], again["dataset_version"])
                self.assertEqual(133, again["quality"]["silver_rows"])
                self.assertEqual(132, storage.read_json(shared["silver_path"] + "/quality.json")["silver_rows"])

            def test_corrupt_snapshot_and_missing_marker_are_refused(self):
                path = shared["silver_path"] + "/quality.json"
                original = storage.read_json(path)
                try:
                    storage.remove(path)
                    write_json(storage, path, {"corrupt": True})
                    with self.assertRaisesRegex(ValueError, "checksum/file inventory"):
                        run_ingest(spark, source, code_sha, base=test_root + "/baseline")
                finally:
                    storage.remove(path)
                    write_json(storage, path, original)
                marker = shared["silver_path"] + "/_SUCCESS"
                storage.remove(marker)
                try:
                    with self.assertRaisesRegex(ValueError, "Incomplete snapshot"):
                        run_ingest(spark, source, code_sha, base=test_root + "/baseline")
                finally:
                    storage.write_bytes(marker, b"")

            def test_bad_header_and_all_invalid_inputs_do_not_publish_silver(self):
                invalid = fixture(Path(temporary.name) / "invalid")
                (invalid / "events.csv").write_text("timestamp,visitorid,event,itemid,transactionid\nbad,,unknown,-1,\n")
                base = test_root + "/invalid"
                version = fingerprint_sources(invalid)["dataset_version"]
                with self.assertRaisesRegex(ValueError, "No valid events"):
                    run_ingest(spark, invalid, code_sha, base=base, require_two_workers=False)
                self.assertTrue(storage.exists(base + "/bronze/retailrocket/" + version + "/_SUCCESS"))
                self.assertFalse(storage.exists(base + "/silver/retailrocket/" + version + "/" + TRANSFORM_VERSION))
                (invalid / "events.csv").write_text("wrong,header\n")
                with self.assertRaisesRegex(ValueError, "Unexpected header"):
                    run_ingest(spark, invalid, code_sha, base=test_root + "/bad-header")
                self.assertFalse(storage.exists(test_root + "/bad-header"))

            def test_hdfs_rename_never_nests_or_overwrites_existing_directory(self):
                parent = test_root + "/rename"
                storage.mkdir(parent + "/source")
                storage.mkdir(parent + "/target")
                storage.write_bytes(parent + "/target/keep", b"keep")
                with self.assertRaises(Exception):
                    storage.rename_no_replace(parent + "/source", parent + "/target")
                self.assertTrue(storage.exists(parent + "/source"))
                self.assertEqual([{"name": "keep", "size": 4}], storage.files(parent + "/target"))

            def test_copy_checksum_failure_prevents_bronze_publication(self):
                original = Hdfs.copy_from_local

                def corrupt_copy(instance, local, remote):
                    original(instance, local, remote)
                    if local.endswith("events.csv"):
                        instance.remove(remote)
                        instance.write_bytes(remote, b"truncated copy")

                base = test_root + "/bad-copy"
                with mock.patch.object(Hdfs, "copy_from_local", corrupt_copy):
                    with self.assertRaisesRegex(ValueError, "do not match fingerprinted"):
                        run_ingest(spark, source, code_sha, base=base)
                self.assertFalse(storage.exists(base + "/bronze/retailrocket/" + shared["dataset_version"]))

        suite = unittest.defaultTestLoader.loadTestsFromTestCase(DataTests)
        outcome = unittest.TextTestRunner(verbosity=2).run(suite)
        if not outcome.wasSuccessful():
            raise RuntimeError(f"Data integration failed: {len(outcome.failures)} failures, {len(outcome.errors)} errors")
        result = {"tests_run": outcome.testsRun, "passed": True, "test_root": test_root,
                  "source_rows": shared["quality"]["source_rows"], "silver_rows": shared["quality"]["silver_rows"],
                  "worker_hosts": sorted(shared["quality"]["worker_rows"]),
                  "minimum_block_replicas": shared["manifest"]["minimum_block_replicas"]}
    finally:
        try:
            # UUID path is generated here, never supplied by caller or read from a manifest.
            storage.remove(test_root)
        finally:
            temporary.cleanup()
            spark.stop()
    if result is not None:
        print("DATA_TEST_RESULT=" + json.dumps({**result, "cleaned": True}, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
