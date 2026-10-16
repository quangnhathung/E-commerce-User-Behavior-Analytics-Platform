"""Small generated fixtures and local storage adapter; no real dataset is copied."""
import csv
from pathlib import Path
import shutil
import sys

APP = Path(__file__).resolve().parents[1]
if str(APP) not in sys.path:
    sys.path.insert(0, str(APP))
from ecommerce_analytics.contracts import HEADERS, sha256_file

BASE_MS = 1430622004384


def fixture(directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    for name, header in HEADERS.items():
        with (directory / name).open("w", newline="") as output:
            writer = csv.writer(output, lineterminator="\n")
            writer.writerow(header)
            if name == "category_tree.csv":
                writer.writerow(["1", ""])
            elif name.startswith("item_properties"):
                writer.writerow([str(BASE_MS), "10", "categoryid", "1"])
    good = [f"{BASE_MS},1,view,10,", f"{BASE_MS+1},1,addtocart,10,",
            f"{BASE_MS+2},1,transaction,10,900", f"{BASE_MS+2},1,transaction,11,900"]
    bad = ["bad,2,view,12,", f"{BASE_MS},3,unknown,13,", f"{BASE_MS},4,transaction,14,",
           f"{BASE_MS},5,view,15,,extra", "", ",".join(HEADERS["events.csv"]),
           '"unterminated', f"{BASE_MS},,view,16,", f"{BASE_MS},6,view,-1,", f"{BASE_MS},7,transaction,17,no"]
    filler = [f"{BASE_MS+1000+i},{100+i},view,{100+i}," for i in range(128)]
    (directory / "events.csv").write_text(",".join(HEADERS["events.csv"]) + "\n" + "\n".join(good + bad + filler + [good[0]]) + "\n")
    return directory


class LocalStorage:
    """Implements the storage boundary with real local file I/O for offline tests."""
    def __init__(self, root):
        self.root = Path(root)

    def local(self, path):
        result = (self.root / path.lstrip("/")).resolve()
        if not result.is_relative_to(self.root.resolve()):
            raise ValueError("Test path escaped its directory")
        return result

    def exists(self, path):
        return self.local(path).exists()

    def mkdir(self, path):
        self.local(path).mkdir(parents=True, exist_ok=True)

    def remove(self, path):
        path = self.local(path)
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink(missing_ok=True)

    def rename_no_replace(self, source, destination):
        if self.exists(destination):
            raise FileExistsError(destination)
        self.local(source).rename(self.local(destination))

    def write_bytes(self, path, value):
        with self.local(path).open("xb") as output:
            output.write(value)

    def read_json(self, path):
        import json
        return json.loads(self.local(path).read_text())

    def files(self, root):
        directory = self.local(root)
        return sorted([{"name": p.relative_to(directory).as_posix(), "size": p.stat().st_size}
                       for p in directory.rglob("*") if p.is_file()], key=lambda entry: entry["name"])

    def sha256(self, path):
        return sha256_file(self.local(path))

    def replicas(self, path):
        return [2] if self.local(path).stat().st_size else []
