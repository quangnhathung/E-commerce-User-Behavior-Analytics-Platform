import hashlib
from pathlib import Path
import tempfile
import unittest

from helpers import LocalStorage
from ecommerce_analytics.snapshots import materialize, verify_snapshot


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.storage = LocalStorage(self.directory.name)
        self.identity = {"kind": "bronze", "dataset_version": "test"}
        self.payload = b"header\nsource bytes\n"

    def build(self, staging):
        self.storage.write_bytes(staging + "/events.csv", self.payload)
        return {"source_files": [{"name": "events.csv", "size": len(self.payload),
                                   "sha256": hashlib.sha256(self.payload).hexdigest()}]}

    def test_publish_and_reuse_do_not_rebuild_or_change_files(self):
        first, reused = materialize(self.storage, "/snapshots/v1", self.identity, self.build)
        self.assertFalse(reused)

        def must_not_build(staging):
            self.fail("Completed snapshot was rebuilt")

        second, reused = materialize(self.storage, "/snapshots/v1", self.identity, must_not_build)
        self.assertTrue(reused)
        self.assertEqual(first, second)
        self.assertEqual(2, second["minimum_block_replicas"])

    def test_failure_before_commit_leaves_no_snapshot_and_keeps_other_data(self):
        self.storage.mkdir("/snapshots/unrelated")
        self.storage.write_bytes("/snapshots/unrelated/keep", b"keep")

        def fault():
            raise RuntimeError("injected failure")

        with self.assertRaisesRegex(RuntimeError, "injected"):
            materialize(self.storage, "/snapshots/v1", self.identity, self.build, before_publish=fault)
        self.assertFalse(self.storage.exists("/snapshots/v1"))
        self.assertEqual(b"keep", self.storage.local("/snapshots/unrelated/keep").read_bytes())
        self.assertEqual(["unrelated"], [p.name for p in self.storage.local("/snapshots").iterdir()])
        materialize(self.storage, "/snapshots/v1", self.identity, self.build)

    def test_corruption_missing_marker_and_identity_mismatch_are_refused(self):
        materialize(self.storage, "/snapshots/v1", self.identity, self.build)
        with self.assertRaisesRegex(ValueError, "identity/version mismatch"):
            verify_snapshot(self.storage, "/snapshots/v1", {**self.identity, "dataset_version": "other"})
        self.storage.local("/snapshots/v1/events.csv").write_bytes(b"corrupt")
        with self.assertRaisesRegex(ValueError, "checksum/file inventory"):
            materialize(self.storage, "/snapshots/v1", self.identity, self.build)
        self.storage.remove("/snapshots/v1/_SUCCESS")
        with self.assertRaisesRegex(ValueError, "Incomplete snapshot"):
            verify_snapshot(self.storage, "/snapshots/v1", self.identity)

    def test_source_changed_during_copy_cannot_publish_bronze(self):
        def bad_copy(staging):
            metadata = self.build(staging)
            self.storage.local(staging + "/events.csv").write_bytes(b"unexpected bytes")
            return metadata
        with self.assertRaisesRegex(ValueError, "do not match fingerprinted"):
            materialize(self.storage, "/snapshots/v1", self.identity, bad_copy)
        self.assertFalse(self.storage.exists("/snapshots/v1"))

    def test_existing_destination_is_never_overwritten_or_nested(self):
        self.storage.mkdir("/snapshots/v1")
        self.storage.write_bytes("/snapshots/v1/keep", b"keep")
        self.storage.mkdir("/snapshots/staging")
        with self.assertRaises(FileExistsError):
            self.storage.rename_no_replace("/snapshots/staging", "/snapshots/v1")
        with self.assertRaisesRegex(ValueError, "Incomplete"):
            materialize(self.storage, "/snapshots/v1", self.identity, self.build)
        self.assertEqual(["keep"], [p.name for p in self.storage.local("/snapshots/v1").iterdir()])


if __name__ == "__main__":
    unittest.main()
