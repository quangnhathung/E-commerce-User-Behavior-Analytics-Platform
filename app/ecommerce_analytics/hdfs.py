"""Small Hadoop JVM adapter. File hashing stays in the JVM with bounded buffers."""
import json


class Hdfs:
    def __init__(self, spark):
        self.jvm = spark._jvm
        self.gateway = spark.sparkContext._gateway
        self.conf = spark._jsc.hadoopConfiguration()
        self.fs = self.jvm.org.apache.hadoop.fs.FileSystem.get(self.conf)
        self.context = self.jvm.org.apache.hadoop.fs.FileContext.getFileContext(self.fs.getUri(), self.conf)

    def path(self, value):
        return self.jvm.org.apache.hadoop.fs.Path(str(value))

    def exists(self, path):
        return self.fs.exists(self.path(path))

    def mkdir(self, path):
        if not self.fs.mkdirs(self.path(path)):
            raise OSError(f"Cannot create HDFS directory: {path}")

    def remove(self, path):
        if self.exists(path) and not self.fs.delete(self.path(path), True):
            raise OSError(f"Cannot remove owned staging/test directory: {path}")

    def rename_no_replace(self, source, destination):
        options = self.gateway.new_array(self.jvm.org.apache.hadoop.fs.Options.Rename, 0)
        self.context.rename(self.path(source), self.path(destination), options)

    def copy_from_local(self, source, destination):
        self.fs.copyFromLocalFile(False, False, self.path(source), self.path(destination))

    def write_bytes(self, path, data):
        stream = self.fs.create(self.path(path), False)
        try:
            stream.write(bytearray(data))
        finally:
            stream.close()

    def read_json(self, path):
        # Metadata only: never read a CSV or Parquet file into driver memory.
        if self.fs.getFileStatus(self.path(path)).getLen() > 8 * 1024 * 1024:
            raise ValueError(f"Metadata file too large: {path}")
        stream = self.fs.open(self.path(path))
        try:
            return json.loads(bytes(stream.readAllBytes()).decode("utf-8"))
        finally:
            stream.close()

    def files(self, root):
        root_path = self.path(root)
        prefix = self.fs.makeQualified(root_path).toUri().getPath().rstrip("/") + "/"
        iterator = self.fs.listFiles(root_path, True)
        result = []
        while iterator.hasNext():
            status = iterator.next()
            relative = status.getPath().toUri().getPath()[len(prefix):]
            result.append({"name": relative, "size": int(status.getLen())})
        return sorted(result, key=lambda entry: entry["name"])

    def sha256(self, path):
        digest = self.jvm.java.security.MessageDigest.getInstance("SHA-256")
        source = self.fs.open(self.path(path))
        target = self.jvm.java.security.DigestOutputStream(self.jvm.java.io.OutputStream.nullOutputStream(), digest)
        self.jvm.org.apache.hadoop.io.IOUtils.copyBytes(source, target, 65536, True)
        return bytes(digest.digest()).hex()

    def replicas(self, path):
        status = self.fs.getFileStatus(self.path(path))
        return [len(set(block.getHosts()))
                for block in self.fs.getFileBlockLocations(status, 0, status.getLen())]
