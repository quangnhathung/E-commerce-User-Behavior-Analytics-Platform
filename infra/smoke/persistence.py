"""Create/verify/clean markers across a real Compose down/up cycle."""
import argparse
import json
import os
import re

from pyspark.sql import SparkSession

from common import consume, create_topic, delete_topic, mongo, publish


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["create", "verify", "clean"])
    parser.add_argument("token")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-f0-9]{32}", args.token):
        raise ValueError("Invalid persistence test token")
    topic = "infra-persist-" + args.token
    database = os.environ["MONGO_DATABASE"]
    spark = SparkSession.builder.appName(topic).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path("/ecommerce/_smoke/persistence/" + args.token)
    record = {"_id": topic, "token": args.token}
    try:
        with mongo() as client:
            collection = client[database].infra_persistence
            if args.action == "create":
                create_topic(topic, partitions=1)
                publish(topic, [record], partitions=1)
                fs.mkdirs(path.getParent())
                stream = fs.create(path, False)
                try:
                    stream.writeUTF(args.token)
                finally:
                    stream.close()
                collection.insert_one(record)
            elif args.action == "verify":
                if consume(topic, 1) != [record]:
                    raise RuntimeError("Kafka persistence marker mismatch")
                stream = fs.open(path)
                try:
                    if stream.readUTF() != args.token:
                        raise RuntimeError("HDFS persistence marker mismatch")
                finally:
                    stream.close()
                if collection.find_one({"_id": topic}) != record:
                    raise RuntimeError("MongoDB persistence marker mismatch")
            else:
                delete_topic(topic)
                fs.delete(path, False)
                collection.delete_one({"_id": topic})
        print("INFRA_RESULT=" + json.dumps({"persistence": args.action, "token": args.token, "ok": True}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
