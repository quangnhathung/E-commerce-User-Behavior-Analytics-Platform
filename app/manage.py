#!/usr/bin/env python3
"""Host-side application CLI; use the same project identity and driver lock as infra."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from infra import manage as infra

ARTIFACTS = ROOT / "artifacts/data"


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    try:
        temporary.write_text(json.dumps(value, sort_keys=True, ensure_ascii=False, indent=2) + "\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def launch(action):
    token = uuid.uuid4().hex
    archive = f"/tmp/ecommerce-code-{token}.zip"
    try:
        infra.execute(infra.compose("exec", "-T", "spark-client", "test", "-f", "/opt/app/package_code.py"), capture=True)
    except infra.InfraError as error:
        raise infra.InfraError("Application source is not mounted in spark-client; run make up after updating Compose. " + str(error)) from error
    packed = json.loads(infra.execute(infra.compose("exec", "-T", "spark-client", "python3", "/opt/app/package_code.py",
                                                   "--output", archive), capture=True))
    script = "ingest.py" if action == "ingest" else "test_data.py"
    prefix = "INGEST_RESULT=" if action == "ingest" else "DATA_TEST_RESULT="
    args = infra.compose("exec", "-T", "spark-client", "python3", "/opt/app/run_job.py", "run", token,
                         "spark-submit", "--deploy-mode", "client", "--py-files", archive,
                         "/opt/app/jobs/" + script, "--code-archive", archive, "--code-sha256", packed["sha256"])
    log_path = ARTIFACTS / "runs" / (token + ".log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    result = None
    stopped = False
    process = None
    try:
        with log_path.open("w") as log:
            process = subprocess.Popen(args, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                       env=infra.child_environment())
            try:
                for line in process.stdout:
                    print(line, end="", flush=True)
                    log.write(line)
                    if line.startswith(prefix):
                        result = json.loads(line[len(prefix):])
                code = process.wait()
                if code or result is None:
                    infra.execute(infra.compose("exec", "-T", "spark-client", "python3", "/opt/app/run_job.py", "cancel", token),
                                  capture=True, timeout=30)
                stopped = True
            except BaseException:
                # A disconnected docker exec client alone need not stop the remote driver.
                infra.execute(infra.compose("exec", "-T", "spark-client", "python3", "/opt/app/run_job.py", "cancel", token),
                              capture=True, timeout=30)
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
            raise infra.InfraError(f"Application job failed (exit {code}); inspect {log_path.relative_to(ROOT)}")
    finally:
        if stopped:
            try:
                infra.execute(infra.compose("exec", "-T", "spark-client", "rm", "-f", archive), capture=True, timeout=20)
            except infra.InfraError as error:
                print(f"NOTE: Could not remove this run's code archive {archive}: {error}", file=sys.stderr)
    result["log"] = str(log_path.relative_to(ROOT))
    if action == "ingest":
        directory = ARTIFACTS / result["dataset_version"] / result["transform_version"]
        for filename, key in (("manifest.json", "manifest"), ("bronze-manifest.json", "bronze_manifest"), ("quality.json", "quality")):
            atomic_json(directory / filename, result[key])
        atomic_json(directory / "result.json", result)
        atomic_json(ARTIFACTS / "latest-ingest.json", {key: result[key] for key in
                    ("dataset_version", "transform_version", "quality_status", "bronze_path", "silver_path", "log")})
    else:
        atomic_json(ARTIFACTS / "test-data.json", result)
    print(f"Application {action} complete; reports in artifacts/data/.")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("ingest", "test-data"))
    args = parser.parse_args()
    if not infra.ENV_FILE.is_file():
        raise infra.InfraError("Missing .env; run make setup first")
    env = infra.read_env(infra.ENV_FILE)
    infra.validate_env(env)
    infra.preflight(env)
    with infra.driver_lock():
        infra.assert_services()
        launch(args.action)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Application job interrupted.", file=sys.stderr)
        sys.exit(130)
    except (infra.InfraError, OSError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
