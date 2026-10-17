"""Read-only host resource observations for this Compose project's running services."""
import json
import threading
import time

from infra import manage as infra


def states():
    ids = [row["ID"] for row in infra.statuses()
           if row["Service"] in infra.LONG_RUNNING and row.get("State") == "running"]
    if len(ids) != len(infra.LONG_RUNNING):
        raise infra.InfraError("Cannot monitor the expected nine running project services")
    template = ('{"id":{{json .Id}},"name":{{json .Name}},"oom_killed":{{json .State.OOMKilled}},'
                '"restart_count":{{json .RestartCount}},"started_at":{{json .State.StartedAt}},'
                '"memory_limit":{{json .HostConfig.Memory}}}')
    output = infra.execute(["docker", "inspect", "--format", template, *ids], capture=True, timeout=20)
    return {row["id"]: row for row in (json.loads(line) for line in output.splitlines() if line)}


class Monitor:
    def __init__(self):
        self.before = states()
        if (any(row["memory_limit"] <= 0 for row in self.before.values()) or
                sum(row["memory_limit"] for row in self.before.values()) > 7 * 1024 ** 3):
            raise infra.InfraError("Actual running container limits exceed the 7 GiB demo budget; apply make up first")
        self.samples, self.errors = [], []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.watch, daemon=True)
        self.started = time.monotonic()

    def watch(self):
        while not self.stop.is_set():
            sample_started = time.monotonic()
            try:
                output = infra.execute(["docker", "stats", "--no-stream", "--format", "{{json .}}", *self.before],
                                       capture=True, timeout=15)
                self.samples.append({"elapsed_seconds": round(time.monotonic() - self.started, 3),
                                     "containers": [json.loads(line) for line in output.splitlines() if line]})
            except Exception as error:
                self.errors.append(type(error).__name__)
            self.stop.wait(max(0, 5 - (time.monotonic() - sample_started)))

    def start(self):
        self.thread.start()

    def finish(self):
        self.stop.set()
        self.thread.join(timeout=20)
        after = states()
        stable = (set(after) == set(self.before) and all(
            not after[key]["oom_killed"] and after[key]["restart_count"] == self.before[key]["restart_count"]
            and after[key]["started_at"] == self.before[key]["started_at"]
            for key in self.before))
        return {"before": self.before, "after": after, "no_oom_or_restart": stable,
                "samples": self.samples, "observation_errors": self.errors,
                "sample_interval_seconds": 5, "memory_observations_complete": not self.errors and bool(self.samples)}
