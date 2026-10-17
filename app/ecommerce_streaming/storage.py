"""Streaming-only Hadoop additions; the historical ingestion package stays frozen."""
from ecommerce_analytics.hdfs import Hdfs
from ecommerce_analytics.snapshots import verify_snapshot, verify_replicas


class Storage(Hdfs):
    def checkpoint_commits(self, root):
        directory = root + "/commits"
        if not self.exists(directory):
            return set()
        return {int(status.getPath().getName()) for status in self.fs.listStatus(self.path(directory))
                if status.isFile() and status.getPath().getName().isdigit()}

    def batch_paths(self, root, through=None):
        if not self.exists(root):
            return []
        paths = []
        for status in self.fs.listStatus(self.path(root)):
            name = status.getPath().getName()
            if status.isDirectory() and len(name) == 20 and name.isdigit() and (through is None or int(name) <= through):
                paths.append(root + "/" + name)
        return sorted(paths)

    def copy_to_local(self, source, destination):
        self.fs.copyToLocalFile(False, self.path(source), self.path(destination), True)

    def checked(self, root, identity):
        manifest = verify_snapshot(self, root, identity)
        verify_replicas(self, root, manifest["files"])
        return manifest


def read_config(storage, prepared_path, expected=None):
    config = storage.read_json(prepared_path + "/config.json")
    if expected is not None and config != expected:
        raise ValueError("Local run config differs from the immutable HDFS copy")
    identity = {"kind": "replay-prepared", "request": config["request"]}
    storage.checked(prepared_path, identity)
    return config
