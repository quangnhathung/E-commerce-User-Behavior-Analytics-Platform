"""Idempotent bootstrap; never silently rotates credentials on an existing volume."""
import os
import time

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure


def main():
    database = os.environ["MONGO_DATABASE"]
    username = os.environ["MONGO_USERNAME"]
    password = os.environ["MONGO_PASSWORD"]
    for attempt in range(30):
        try:
            with MongoClient("mongodb", username=os.environ["MONGO_ROOT_USERNAME"],
                             password=os.environ["MONGO_ROOT_PASSWORD"], authSource="admin",
                             serverSelectionTimeoutMS=4000) as root:
                root.admin.command("ping")
                target = root[database]
                if not target.command("usersInfo", username)["users"]:
                    target.command("createUser", username, pwd=password,
                                   roles=[{"role": "readWrite", "db": database}])
                target.infra_metadata.update_one({"_id": "bootstrap-v1"},
                                                {"$setOnInsert": {"schema_version": 1}}, upsert=True)
            with MongoClient("mongodb", username=username, password=password, authSource=database,
                             serverSelectionTimeoutMS=4000) as application:
                application[database].infra_metadata.find_one({"_id": "bootstrap-v1"})
            print("MongoDB application account and bootstrap metadata verified.")
            return
        except ConnectionFailure:
            if attempt == 29:
                raise
            time.sleep(2)
        except OperationFailure as error:
            raise RuntimeError("MongoDB authentication/authorization failed; restore matching credentials or rotate explicitly.") from error


if __name__ == "__main__":
    main()
