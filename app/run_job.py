"""Run/cancel one UUID-owned Spark process group inside spark-client."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time


def process_start(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return None if fields[0] == "Z" else fields[19]
    except FileNotFoundError:
        return None


def run(token, command):
    record = Path(f"/tmp/ecommerce-job-{token}.json")
    child = None
    cancelled = False

    def terminate(signum, frame):
        nonlocal cancelled
        cancelled = True
        if child is not None and child.poll() is None:
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    with record.open("x") as output:
        json.dump({"pid": os.getpid(), "start": process_start(os.getpid())}, output)
    try:
        if cancelled:
            return 130
        child = subprocess.Popen(["timeout", "--kill-after=15s", "1800s", *command], start_new_session=True)
        if cancelled:
            terminate(signal.SIGTERM, None)
        code = child.wait()
        return 130 if cancelled else code
    finally:
        if child is not None and child.poll() is None:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait()
        record.unlink(missing_ok=True)


def cancel(token):
    record = Path(f"/tmp/ecommerce-job-{token}.json")
    try:
        info = json.loads(record.read_text())
    except FileNotFoundError:
        return
    if process_start(info["pid"]) != info["start"]:
        return
    try:
        os.kill(info["pid"], signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 20
    while process_start(info["pid"]) == info["start"]:
        if time.monotonic() >= deadline:
            raise RuntimeError("Remote job is still stopping; retain its archive and inspect the container")
        time.sleep(0.1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("run", "cancel"))
    parser.add_argument("token")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-f0-9]{32}", args.token):
        parser.error("Expected a UUID hex token")
    if args.action == "cancel":
        cancel(args.token)
    else:
        if not args.command:
            parser.error("Missing command")
        sys.exit(run(args.token, args.command))
