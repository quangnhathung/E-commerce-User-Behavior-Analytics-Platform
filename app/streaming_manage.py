#!/usr/bin/env python3
"""Host CLI for bounded streaming demos; Python standard library only."""
import argparse
from contextlib import contextmanager, nullcontext
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid

APP = Path(__file__).resolve().parent
ROOT = APP.parent
sys.path[:0] = [str(APP), str(ROOT)]
from infra import manage as infra
from manage import atomic_json
from ecommerce_analytics.contracts import sha256_file
from ecommerce_streaming.contracts import DEFAULT_END, DEFAULT_START, PIPELINE_VERSION, validate_request
from streaming_monitor import Monitor

ARTIFACTS = ROOT / "artifacts/streaming"
SPARK_ACTIONS = {"prepare-demo", "stream", "test-stream"}


@contextmanager
def producer_lock():
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    with (ARTIFACTS / "producer.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise infra.InfraError("Another managed replay producer is running") from error
        try:
            if (ARTIFACTS / "producer-unconfirmed.json").exists():
                raise infra.InfraError("A remote producer was not confirmed stopped; inspect producer-unconfirmed.json "
                                       "and confirm it has stopped before removing that marker")
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def run_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", value):
        raise ValueError("Invalid RUN_ID")
    return value


def upload_spec(path, spec):
    # A bounded run can still have megabytes of minute reference data. Keep it
    # off argv (Linux limits a single argument to approximately 128 KiB).
    script = ("import os,sys; "
              "f=os.fdopen(os.open(sys.argv[1],os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w'); "
              "f.write(sys.stdin.read()); f.close()")
    result = subprocess.run(infra.compose("exec", "-T", "spark-client", "python3", "-c", script, path),
                            input=json.dumps(spec), text=True, capture_output=True, cwd=ROOT,
                            env=infra.child_environment(), timeout=30)
    if result.returncode:
        raise infra.InfraError("Could not upload this run's private control specification")


def launch(action, specification, directory, *, stdin=None, quiet=False):
    token = uuid.uuid4().hex
    archive = f"/tmp/ecommerce-stream-{token}.zip"
    spec_path = f"/tmp/ecommerce-stream-{token}.json"
    process, stopped = None, False
    monitor = Monitor() if action in ("stream", "test-stream") else None
    try:
        packed = json.loads(infra.execute(infra.compose("exec", "-T", "spark-client", "python3", "/opt/app/package_code.py",
                            "--output", archive, "--profile", "streaming"), capture=True, timeout=30))
        spec = specification(packed["sha256"]) if callable(specification) else specification
        upload_spec(spec_path, spec)
        command = (["spark-submit", "--deploy-mode", "client", "--py-files", archive]
                   if action in SPARK_ACTIONS else ["python3"])
        args = infra.compose("exec", "-T", "spark-client", "python3", "/opt/app/run_job.py", "run", token,
            *command, "/opt/app/jobs/streaming.py", action, "--spec-file", spec_path,
            "--code-archive", archive, "--code-sha256", packed["sha256"])
        directory.mkdir(parents=True, exist_ok=True)
        log_path = directory / "logs" / f"{action}-{token}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
    except BaseException:
        try:
            infra.execute(infra.compose("exec", "-T", "spark-client", "rm", "-f", archive, spec_path), capture=True, timeout=20)
        except infra.InfraError:
            pass
        raise
    result = None

    def stop_remote():
        marker = (infra.ARTIFACTS / "driver-unconfirmed.json" if action in SPARK_ACTIONS else
                  ARTIFACTS / "producer-unconfirmed.json" if action == "replay" else None)
        try:
            infra.execute(infra.compose("exec", "-T", "spark-client", "python3", "/opt/app/run_job.py", "cancel", token),
                          capture=True, timeout=30)
        except BaseException:
            if marker is not None:
                atomic_json(marker, {"token": token, "archive": archive, "spec_file": spec_path,
                                     "action": action, "log": str(log_path.relative_to(ROOT))})
            raise
        if marker is not None and marker.exists() and json.loads(marker.read_text()).get("token") == token:
            marker.unlink()

    try:
        if monitor is not None:
            monitor.start()
        with log_path.open("w") as log:
            process = subprocess.Popen(args, cwd=ROOT, env=infra.child_environment(), stdin=stdin or subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            try:
                for line in process.stdout:
                    log.write(line)
                    log.flush()
                    if not quiet:
                        print(line, end="", flush=True)
                    if line.startswith("STREAMING_RESULT="):
                        result = json.loads(line.partition("=")[2])
                    elif line.startswith("REPLAY_PROGRESS="):
                        atomic_json(directory / "ack.json", json.loads(line.partition("=")[2]))
                code = process.wait()
                if code or result is None:
                    stop_remote()
                stopped = True
            except BaseException:
                stop_remote()
                stopped = True
                raise
            finally:
                process.stdout.close()
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
        if code or result is None:
            raise infra.InfraError(f"{action} failed (exit {code}); inspect {log_path.relative_to(ROOT)}")
    finally:
        resource_report = None
        try:
            if monitor is not None and monitor.thread.ident is not None:
                try:
                    resource_report = monitor.finish()
                except Exception as error:
                    resource_report = {"no_oom_or_restart": False, "memory_observations_complete": False,
                                       "error_type": type(error).__name__}
                atomic_json(directory / "logs" / f"resources-{token}.json", resource_report)
                atomic_json(directory / "resources.json", resource_report)
        finally:
            if stopped or process is None:
                try:
                    infra.execute(infra.compose("exec", "-T", "spark-client", "rm", "-f", archive, spec_path), capture=True, timeout=20)
                except infra.InfraError as error:
                    print(f"NOTE: Retained owned archive {archive}: {error}", file=sys.stderr)
    if resource_report is not None:
        result["resources"] = {key: resource_report[key] for key in
                               ("no_oom_or_restart", "memory_observations_complete")}
        if not resource_report["no_oom_or_restart"]:
            raise infra.InfraError("Project container restarted or was OOM-killed; inspect resources.json")
    result["log"] = str(log_path.relative_to(ROOT))
    return result


def fetch_replay(remote, local, checksum):
    temporary = local.with_name(local.name + ".tmp-" + uuid.uuid4().hex)
    try:
        with temporary.open("wb") as output:
            result = subprocess.run(infra.compose("exec", "-T", "spark-client", "cat", remote), cwd=ROOT,
                                    env=infra.child_environment(), stdout=output, stderr=subprocess.PIPE, timeout=60)
        if result.returncode or sha256_file(temporary) != checksum:
            raise infra.InfraError("Replay download failed or checksum differs; retry prepare-demo with the same RUN_ID")
        os.replace(temporary, local)
    finally:
        temporary.unlink(missing_ok=True)


def prepare_demo(args):
    identity = run_id(args.run_id or "replay-" + uuid.uuid4().hex)
    directory = ARTIFACTS / identity
    latest = json.loads((ROOT / "artifacts/data/latest-ingest.json").read_text())
    remote = "/tmp/ecommerce-replay-" + uuid.uuid4().hex + ".jsonl"

    def spec(code_hash):
        request = {"run_id": identity, "start": args.start, "end": args.end, "rate": args.rate,
                   "pipeline_version": PIPELINE_VERSION, "code_sha256": code_hash,
                   "dataset_version": latest["dataset_version"], "silver_path": latest["silver_path"],
                   "base": "/ecommerce", "topic": "ecommerce.events.raw", "dlq_topic": "ecommerce.events.dlq"}
        validate_request(request)
        return {"request": request, "local_output": remote}

    result = None
    try:
        result = launch("prepare-demo", spec, directory)
        config = result["config"]
        fetch_replay(remote, directory / "events.jsonl", config["replay_sha256"])
        atomic_json(directory / "config.json", config)
        atomic_json(directory / "manifest.json", result["manifest"])
        atomic_json(directory / "prepare.json", result)
        atomic_json(ARTIFACTS / "latest-run.json", {"run_id": identity})
    finally:
        # Only the UUID path created by this invocation, never the prepared HDFS artifact.
        if result is not None:
            try:
                infra.execute(infra.compose("exec", "-T", "spark-client", "rm", "-f", remote), capture=True, timeout=20)
            except infra.InfraError:
                print(f"NOTE: Could not clean owned temporary replay {remote}", file=sys.stderr)
        else:
            print(f"NOTE: If present, retained temporary replay {remote}; inspect the failed run before cleanup.", file=sys.stderr)
    print(f"Prepared RUN_ID={identity}; start make stream and make replay in separate terminals.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare-demo", "stream", "replay", "stream-status", "test-stream"))
    parser.add_argument("--run-id", default=os.environ.get("RUN_ID") or None)
    parser.add_argument("--start", default=os.environ.get("START") or DEFAULT_START)
    parser.add_argument("--end", default=os.environ.get("END") or DEFAULT_END)
    parser.add_argument("--rate", type=float, default=os.environ.get("RATE") or 10)
    args = parser.parse_args()
    env = infra.read_env(infra.ENV_FILE)
    infra.validate_env(env)
    if args.action != "stream-status":
        infra.preflight(env)
    lock = infra.driver_lock() if args.action in SPARK_ACTIONS else producer_lock() if args.action == "replay" else nullcontext()
    with lock:
        if args.action != "stream-status":
            infra.assert_services()
        if args.action == "prepare-demo":
            prepare_demo(args)
            return
        if args.action == "test-stream":
            result = launch(args.action, {}, ARTIFACTS / "tests")
            atomic_json(ARTIFACTS / "test-stream.json", result)
            return
        identity = run_id(args.run_id or json.loads((ARTIFACTS / "latest-run.json").read_text())["run_id"])
        directory = ARTIFACTS / identity
        config = json.loads((directory / "config.json").read_text())
        if config["run_id"] != identity:
            raise ValueError("Local run config has the wrong RUN_ID")
        if args.action == "replay":
            source = directory / "events.jsonl"
            if sha256_file(source) != config["replay_sha256"]:
                raise ValueError("Replay file checksum differs from its prepared config")
            state = launch("stream-status", {"config": config}, directory, quiet=True)
            with source.open("rb") as input_file:
                for _ in range(state["producer"]["last_seq"] + 1):
                    if not input_file.readline():
                        raise ValueError("Producer cursor exceeds replay file")
                result = launch("replay", {"config": config}, directory, stdin=input_file)
        else:
            result = launch(args.action, {"config": config}, directory)
        filename = {"stream": "report.json", "replay": "producer.json", "stream-status": "status.json"}[args.action]
        atomic_json(directory / filename, result)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Streaming command interrupted; committed data/checkpoints are retained.", file=sys.stderr)
        sys.exit(130)
    except subprocess.TimeoutExpired:
        print("ERROR: Docker control/copy command timed out; inspect this attempt's log and temporary files.", file=sys.stderr)
        sys.exit(1)
    except (infra.InfraError, OSError, ValueError, KeyError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
