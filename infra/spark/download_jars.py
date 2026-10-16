"""Install Kafka connector JARs at build time with locked SHA-512 checksums.

Use Spark's bundled Hadoop runtime, spark-tags, jsr305, compression and SLF4J.
"""
import hashlib
import time
import urllib.request
from pathlib import Path

JARS = [
    ("org/apache/spark", "spark-sql-kafka-0-10_2.12", "3.5.7", "343946b39720b90258d102a0820393103b43ee48c01292bda30ee0345c2429fcc7c9c0990cadcac94dd3bd99b4cd25ae39d62a8a6cf6ce25f074948e9d88a895"),
    ("org/apache/spark", "spark-token-provider-kafka-0-10_2.12", "3.5.7", "e5389ac89f1865a53cd5cfdf07e2d5b44e2ba94780b71795ab575bf1e73e0550b988a76dd91fd28a36c0e97fb5bc018ad63f4b3dfe8c050913b8afec47594fe0"),
    ("org/apache/kafka", "kafka-clients", "3.4.1", "b111e1980fd6bf65ca546f6b517fb31a140b19b4dc88646e723b11b54b521e48643f33aa341f47b3111a4556af131200f80c851e1b68d37cb4e747e283eefd7e"),
    ("org/apache/commons", "commons-pool2", "2.11.1", "7177d8a9e45ae47881ec323150dc7bd3372ce32eff9866029b47a77cb734535d5e987f22e6cb7009104832c3772079fb6322b2919650765b76a4b4aa8eae648e"),
]


def main():
    target = Path("/opt/spark/jars")
    for dependency in ("hadoop-client-runtime", "spark-tags_2.12", "jsr305", "lz4-java", "snappy-java", "zstd-jni", "slf4j-api"):
        if not list(target.glob(dependency + "-*.jar")):
            raise RuntimeError(f"Spark distribution missing bundled dependency: {dependency}")
    for group, artifact, version, checksum in JARS:
        filename = f"{artifact}-{version}.jar"
        url = f"https://repo.maven.apache.org/maven2/{group}/{artifact}/{version}/{filename}"
        for attempt in range(4):
            try:
                with urllib.request.urlopen(url, timeout=90) as response:
                    data = response.read()
                if hashlib.sha512(data).hexdigest() != checksum:
                    raise ValueError(f"SHA-512 mismatch: {filename}")
                (target / filename).write_bytes(data)
                break
            except OSError:
                if attempt == 3:
                    raise
                time.sleep(2 ** attempt)


if __name__ == "__main__":
    main()
