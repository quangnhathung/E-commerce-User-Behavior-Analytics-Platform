#!/usr/bin/env python3
"""Host-side Compose lifecycle. Uses only the Python standard library."""
import argparse
import base64
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import platform
import re
import secrets
import shlex
import shutil
import socket
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env"
ARTIFACTS = ROOT / "artifacts" / "infra"
PORT_KEYS = ("KAFKA_HOST_PORT", "MONGO_HOST_PORT", "HDFS_UI_PORT", "SPARK_MASTER_UI_PORT",
             "SPARK_WORKER_1_UI_PORT", "SPARK_WORKER_2_UI_PORT", "SPARK_APPLICATION_UI_PORT")
LONG_RUNNING = {"kafka", "namenode", "datanode-1", "datanode-2", "spark-master",
                "spark-worker-1", "spark-worker-2", "spark-client", "mongodb"}
INIT_SERVICES = {"kafka-init", "hdfs-init", "mongo-init"}


class InfraError(RuntimeError):
    pass


def read_env(path):
    result = {}
    for number, line in enumerate(path.read_text().splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
            raise InfraError(f"Invalid environment assignment at line {number}")
        if key in result:
            raise InfraError(f"Duplicate environment variable: {key}")
        values = shlex.split(value, comments=True)
        if len(values) > 1:
            raise InfraError(f"Quote values containing spaces: {key}")
        result[key] = values[0] if values else ""
    return result


def validate_env(env, template=None):
    required = set(template or read_env(ROOT / ".env.example"))
    missing = sorted(key for key in required if not env.get(key) or env[key] == "__GENERATE__")
    if missing:
        raise InfraError("Missing/uninitialized environment values: " + ", ".join(missing))
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", env["COMPOSE_PROJECT_NAME"]):
        raise InfraError("Invalid COMPOSE_PROJECT_NAME")
    if not re.fullmatch(r"[A-Za-z0-9_-]{22}", env["KAFKA_CLUSTER_ID"]):
        raise InfraError("KAFKA_CLUSTER_ID must be a 22-character Kafka UUID")
    if not re.fullmatch(r"CID-[A-Za-z0-9_-]+", env["HDFS_CLUSTER_ID"]):
        raise InfraError("HDFS_CLUSTER_ID must start with CID- and contain letters/digits/-/_")
    ports = []
    for key in PORT_KEYS:
        try:
            value = int(env[key])
        except ValueError as error:
            raise InfraError(f"Invalid port: {key}") from error
        if not 1024 <= value <= 65535:
            raise InfraError(f"Port outside 1024..65535: {key}")
        ports.append(value)
    if len(set(ports)) != len(ports):
        raise InfraError("Published host ports must be distinct")
    for key in ("MIN_AVAILABLE_RAM_GIB", "MIN_DOCKER_DISK_GIB"):
        try:
            valid = float(env[key]) >= 1
        except ValueError:
            valid = False
        if not valid:
            raise InfraError(f"{key} must be at least 1")
    for key in ("KAFKA_IMAGE", "MONGO_IMAGE"):
        if not re.search(r"@sha256:[a-f0-9]{64}$", env[key]):
            raise InfraError(f"{key} must be pinned by SHA-256 digest")


def setup(root=None):
    root = root or ROOT
    target = root / ".env"
    template_path = root / ".env.example"
    template = read_env(template_path)
    if target.exists():
        validate_env(read_env(target), template)
        print("Existing .env preserved; credentials and cluster identities unchanged.")
        return
    generated = {
        "KAFKA_CLUSTER_ID": base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("="),
        "MONGO_ROOT_PASSWORD": secrets.token_hex(24),
        "MONGO_PASSWORD": secrets.token_hex(24),
    }
    lines = []
    for line in template_path.read_text().splitlines():
        key = line.partition("=")[0]
        lines.append(f"{key}={generated[key]}" if key in generated else line)
    content = "\n".join(lines) + "\n"
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as output:
        output.write(content)
    validate_env(read_env(target), template)
    print("Created .env (mode 0600); credentials are not printed.")


def execute(args, *, capture=False, timeout=None):
    try:
        result = subprocess.run(args, cwd=ROOT, text=True, capture_output=capture, timeout=timeout,
                                env=child_environment())
    except FileNotFoundError as error:
        raise InfraError(f"Missing command: {args[0]}") from error
    except subprocess.TimeoutExpired as error:
        raise InfraError(f"Command timed out: {args[0]}") from error
    if result.returncode:
        detail = (result.stderr or "").strip() if capture else ""
        # Do not include arguments: configuration or credentials may be in them.
        raise InfraError(f"{args[0]} failed (exit {result.returncode})" + (f": {detail}" if detail else ""))
    return result.stdout if capture else ""


def child_environment():
    """Use .env as the sole source of project variables, preserving Docker context settings."""
    environment = os.environ.copy()
    if ENV_FILE.is_file():
        for key in read_env(ENV_FILE):
            environment.pop(key, None)
    return environment


def compose(*arguments):
    return ["docker", "compose", "--project-directory", str(ROOT), "--env-file", str(ENV_FILE),
            "-f", str(ROOT / "compose.yaml"), *arguments]


def parse_ps(output):
    output = output.strip()
    if not output:
        return []
    if output.startswith("["):
        return json.loads(output)
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def configuration():
    return json.loads(execute(compose("config", "--format", "json"), capture=True, timeout=30))


def statuses():
    return parse_ps(execute(compose("ps", "--all", "--format", "json"), capture=True, timeout=30))


def check_memory_budget(env, services, running, *, available, daemon_total):
    """Check configured limits including init, then the budget still needed on the host."""
    minimum = float(env["MIN_AVAILABLE_RAM_GIB"]) * 1024 ** 3
    configured = sum(int(service.get("mem_limit", 0)) for service in services.values())
    if configured > minimum:
        raise InfraError(f"Configured container memory limits total {configured / 1024 ** 3:.2f} GiB, "
                         f"exceeding MIN_AVAILABLE_RAM_GIB={minimum / 1024 ** 3:.2f}. "
                         "Reduce container limits or raise the memory budget.")
    reserved = sum(int(services[name].get("mem_limit", 0))
                   for name in running if name in LONG_RUNNING and name in services)
    needed = max(0, minimum - reserved)
    if daemon_total < minimum:
        raise InfraError("Docker daemon memory is below MIN_AVAILABLE_RAM_GIB")
    if available < needed:
        raise InfraError(f"Only {available / 1024 ** 3:.1f} GiB available; need {needed / 1024 ** 3:.1f} GiB "
                         "for the remaining budget. Free host memory before starting.")


def preflight(env, *, resources=True, ports=True):
    if platform.system() != "Linux" or platform.machine() not in ("x86_64", "amd64"):
        raise InfraError("This infrastructure targets Linux amd64")
    execute(["docker", "compose", "version"], capture=True, timeout=20)
    try:
        info = json.loads(execute(["docker", "info", "--format", "{{json .}}"], capture=True, timeout=30))
    except InfraError as error:
        raise InfraError("Docker daemon is unavailable to this session. Run docker info in the execution environment; "
                         "check the selected context, daemon and socket access. No host permissions were changed. "
                         + str(error)) from error
    if info.get("Architecture") not in ("x86_64", "amd64"):
        raise InfraError("Docker daemon must use amd64")
    if info.get("NCPU", 0) < 4:
        raise InfraError("Docker daemon needs at least four logical CPUs")
    cpuinfo = Path("/proc/cpuinfo").read_text()
    if not any(line.startswith("flags") and "avx" in line.split() for line in cpuinfo.splitlines()):
        raise InfraError("MongoDB 8 requires AVX support on this amd64 host")
    dataset = Path(env["DATASET_PATH"])
    if not dataset.is_absolute():
        dataset = ROOT / dataset
    for filename in ("events.csv", "category_tree.csv", "item_properties_part1.csv", "item_properties_part2.csv"):
        if not (dataset / filename).is_file() or not os.access(dataset / filename, os.R_OK):
            raise InfraError(f"Dataset file is missing/unreadable: {filename}")
    config = configuration()
    current = statuses()
    running = {row["Service"] for row in current if row.get("State") == "running"}
    if resources:
        available = next(int(line.split()[1]) * 1024 for line in Path("/proc/meminfo").read_text().splitlines()
                         if line.startswith("MemAvailable:"))
        check_memory_budget(env, config["services"], running,
                            available=available, daemon_total=info.get("MemTotal", 0))
    docker_root = Path(info.get("DockerRootDir", "/var/lib/docker"))
    try:
        free = shutil.disk_usage(docker_root).free
    except OSError:
        print("NOTE: Docker data partition is not visible here; verify free disk on the Docker host.")
    else:
        if free < float(env["MIN_DOCKER_DISK_GIB"]) * 1024 ** 3:
            raise InfraError("Docker data partition has insufficient free disk")
    if ports:
        owned_ports = {publisher["PublishedPort"] for row in current if row.get("State") == "running"
                       for publisher in (row.get("Publishers") or [])}
        for key in PORT_KEYS:
            port = int(env[key])
            if port in owned_ports:
                continue
            with socket.socket() as probe:
                try:
                    probe.bind(("127.0.0.1", port))
                except OSError as error:
                    raise InfraError(f"Host port {port} ({key}) is occupied by another service") from error
    print("Preflight passed: Docker, architecture, dataset, configuration, disk and requested resource/port checks.")


def assert_services():
    rows = {row["Service"]: row for row in statuses()}
    for name in LONG_RUNNING:
        row = rows.get(name, {})
        if row.get("State") != "running" or row.get("Health") != "healthy":
            raise InfraError(f"Service not healthy: {name}")
    for name in INIT_SERVICES:
        row = rows.get(name, {})
        if row.get("State") != "exited" or row.get("ExitCode") != 0:
            raise InfraError(f"Bootstrap job did not complete successfully: {name}")
    execute(compose("exec", "-T", "spark-client", "python3", "/opt/infra/smoke/readiness.py", "--timeout", "120"))


def start(env):
    preflight(env)
    execute(compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "300"))
    assert_services()
    print("Infrastructure ready: nine healthy services and three completed init jobs.")


@contextmanager
def driver_lock():
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    with (ARTIFACTS / "driver.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise InfraError("Another managed Spark job is running (smoke, persistence or application)") from error
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def spark_job(script, *arguments, report_name):
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    args = compose("exec", "-T", "spark-client", "timeout", "--kill-after=15s", "600s", "spark-submit", "--deploy-mode", "client",
                   "/opt/infra/smoke/" + script, *arguments)
    result = None
    with (ARTIFACTS / (report_name + ".log")).open("w") as log:
        with subprocess.Popen(args, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              env=child_environment()) as process:
            for line in process.stdout:
                print(line, end="", flush=True)
                log.write(line)
                if line.startswith("INFRA_RESULT="):
                    result = json.loads(line.partition("=")[2])
            code = process.wait()
    if code or result is None:
        raise InfraError(f"Spark job failed; inspect artifacts/infra/{report_name}.log")
    (ARTIFACTS / (report_name + ".json")).write_text(json.dumps(result, indent=2) + "\n")
    return result


def versions():
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    config = configuration()
    images = sorted({service["image"] for service in config["services"].values()})
    inspected = json.loads(execute(["docker", "image", "inspect", *images], capture=True, timeout=30))
    data = {"docker": json.loads(execute(["docker", "version", "--format", "{{json .}}"], capture=True)),
            "compose": execute(["docker", "compose", "version", "--short"], capture=True).strip(),
            "images": [{key: image.get(key) for key in ("Id", "RepoTags", "RepoDigests", "Architecture", "Os")}
                       for image in inspected]}
    (ARTIFACTS / "versions.json").write_text(json.dumps(data, indent=2) + "\n")
    print("Wrote artifacts/infra/versions.json (no environment/credentials included).")


def require_buildkit():
    if os.environ.get("DOCKER_BUILDKIT") == "0":
        raise InfraError("BuildKit is required for download cache mounts. Unset DOCKER_BUILDKIT=0 before building.")
    try:
        execute(["docker", "buildx", "version"], capture=True, timeout=20)
    except InfraError as error:
        raise InfraError("Docker Buildx plugin is required before building. On Arch Linux: "
                         "sudo pacman -S docker-buildx; then check docker buildx version. "
                         "See docs/infra/09-build-optimization.md. " + str(error)) from error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["setup", "build", "up", "doctor", "smoke", "persistence-test",
                                          "ps", "logs", "down", "reset", "config", "versions"])
    action = parser.parse_args().action
    if action == "setup":
        setup()
        return
    if not ENV_FILE.is_file():
        raise InfraError("Missing .env; run make setup first")
    env = read_env(ENV_FILE)
    validate_env(env)
    if action == "config":
        execute(compose("config", "--quiet"))
        print("Compose configuration valid (credentials not printed).")
    elif action == "build":
        require_buildkit()
        preflight(env, resources=False, ports=False)
        execute(compose("pull", "kafka", "mongodb"))
        execute(compose("build", "--pull", "namenode", "spark-master"))
        versions()
    elif action == "up":
        start(env)
    elif action == "doctor":
        preflight(env)
        if statuses():
            assert_services()
        else:
            print("Cluster not started yet; run make build, then make up.")
    elif action == "smoke":
        with driver_lock():
            assert_services()
            spark_job("pipeline.py", report_name="smoke")
    elif action == "persistence-test":
        with driver_lock():
            assert_services()
            token = uuid.uuid4().hex
            (ARTIFACTS / "persistence-token.txt").write_text(token + "\n")
            spark_job("persistence.py", "create", token, report_name="persistence-create")
            execute(compose("down", "--timeout", "60"))
            start(env)
            spark_job("persistence.py", "verify", token, report_name="persistence-verify")
            spark_job("persistence.py", "clean", token, report_name="persistence-clean")
            print("Kafka, HDFS and MongoDB markers survived Compose down/up.")
    elif action in ("ps", "logs"):
        execute(compose("ps", "--all") if action == "ps" else compose("logs", "--tail", "100"))
    elif action == "down":
        execute(compose("down", "--timeout", "60"))
    elif action == "reset":
        if os.environ.get("CONFIRM") != "delete-project-data":
            raise InfraError("Reset deletes this project's data. Use make reset CONFIRM=delete-project-data")
        execute(compose("down", "--volumes", "--timeout", "60"))
    elif action == "versions":
        versions()


if __name__ == "__main__":
    try:
        main()
    except (InfraError, ValueError, OSError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
