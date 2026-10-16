"""Immutable, checked snapshots; storage adapter is also usable by local tests."""
from datetime import datetime, timezone
import json
import posixpath
import time
import uuid
import warnings

MANIFEST_VERSION = 1


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def write_json(storage, path, value):
    storage.write_bytes(path, (json.dumps(value, sort_keys=True, ensure_ascii=False, indent=2,
                                        allow_nan=False) + "\n").encode("utf-8"))


def payload_files(storage, root):
    return [entry for entry in storage.files(root)
            if entry["name"] != "manifest.json" and posixpath.basename(entry["name"]) != "_SUCCESS"]


def inventory(storage, root):
    return [{**entry, "sha256": storage.sha256(root + "/" + entry["name"])}
            for entry in payload_files(storage, root)]


def verify_snapshot(storage, root, identity):
    if not storage.exists(root + "/_SUCCESS"):
        raise ValueError(f"Incomplete snapshot (no completion marker): {root}")
    manifest = storage.read_json(root + "/manifest.json")
    if manifest.get("manifest_version") != MANIFEST_VERSION or manifest.get("identity") != identity:
        raise ValueError(f"Snapshot identity/version mismatch: {root}; do not overwrite, use a new transform version")
    if manifest.get("files") != inventory(storage, root):
        raise ValueError(f"Snapshot checksum/file inventory mismatch: {root}; restore or investigate the snapshot")
    return manifest


def verify_replicas(storage, root, files, timeout=90):
    deadline = time.monotonic() + timeout
    while True:
        counts = [count for entry in files for count in storage.replicas(root + "/" + entry["name"])]
        if counts and min(counts) >= 2:
            return min(counts)
        if time.monotonic() >= deadline:
            raise ValueError(f"Data blocks have not reached two replicas: {root}")
        time.sleep(2)


def materialize(storage, destination, identity, build, *, before_publish=None):
    if storage.exists(destination):
        manifest = verify_snapshot(storage, destination, identity)
        verify_replicas(storage, destination, manifest["files"])
        return manifest, True
    parent, name = posixpath.split(destination)
    storage.mkdir(parent)
    staging = parent + "/." + name + ".tmp-" + uuid.uuid4().hex
    storage.mkdir(staging)
    try:
        metadata = build(staging)
        files = inventory(storage, staging)
        if identity["kind"] == "bronze" and files != metadata["source_files"]:
            raise ValueError("Copied Bronze bytes do not match fingerprinted source files")
        minimum = verify_replicas(storage, staging, files)
        manifest = {"manifest_version": MANIFEST_VERSION, "identity": identity,
                    "created_at": utc_now(), "snapshot_path": destination,
                    "minimum_block_replicas": minimum, "files": files, "metadata": metadata}
        write_json(storage, staging + "/manifest.json", manifest)
        storage.write_bytes(staging + "/_SUCCESS", b"")
        if before_publish is not None:
            before_publish()
        storage.rename_no_replace(staging, destination)
        return manifest, False
    finally:
        try:
            storage.remove(staging)  # Only the UUID-owned directory; never the destination.
        except Exception as error:
            warnings.warn(f"Owned staging cleanup failed: {staging}: {error}")
