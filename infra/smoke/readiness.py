"""Check real service readiness from the same network/identity as future jobs."""
import argparse
import json
import os
import time
import urllib.request

from common import kafka, mongo


def get_json(url):
    with urllib.request.urlopen(url, timeout=3) as response:
        return json.load(response)


def check_spark_workers(master):
    workers = [worker for worker in master.get("workers", []) if worker.get("state") == "ALIVE"]
    if len(workers) != 2:
        raise RuntimeError(f"Expected 2 live Spark workers, got {len(workers)}")
    if any(worker.get("cores", 0) != 1 or worker.get("memory", 0) < 640 for worker in workers):
        raise RuntimeError("Spark worker resources do not match 1 core / 640 MiB")
    return len(workers)


def check():
    worker_count = check_spark_workers(get_json("http://spark-master:18080/json/"))
    state = get_json("http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState")
    if not state.get("beans") or state["beans"][0].get("NumLiveDataNodes") != 2:
        raise RuntimeError("HDFS does not have exactly two live DataNodes")
    metadata = kafka().list_topics(timeout=4)
    for name, partitions in (("ecommerce.events.raw", 3), ("ecommerce.events.dlq", 1)):
        topic = metadata.topics.get(name)
        if topic is None or topic.error is not None or len(topic.partitions) != partitions:
            raise RuntimeError(f"Kafka topic configuration mismatch: {name}")
        if any(len(part.replicas) != 1 for part in topic.partitions.values()):
            raise RuntimeError(f"Unexpected replication factor: {name}")
    with mongo() as client:
        marker = client[os.environ["MONGO_DATABASE"]].infra_metadata.find_one({"_id": "bootstrap-v1"})
        if not marker:
            raise RuntimeError("MongoDB bootstrap marker missing")
    return {"spark_workers": worker_count, "hdfs_datanodes": 2, "kafka_topics": 2, "mongo_authenticated": True}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()
    deadline = time.monotonic() + (0 if args.once else args.timeout)
    while True:
        try:
            print(json.dumps(check()))
            return
        except Exception as error:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Cluster readiness failed ({type(error).__name__}): {error}") from error
            time.sleep(2)


if __name__ == "__main__":
    main()
