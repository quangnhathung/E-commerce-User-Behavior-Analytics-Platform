"""Offline checks: real Compose expansion plus bootstrap invariants, no daemon needed."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import unittest
from unittest import mock
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("infra_manage", ROOT / "infra/manage.py")
manage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(manage)


class BootstrapTests(unittest.TestCase):
    def test_build_rejects_disabled_buildkit_without_contacting_docker(self):
        with mock.patch.dict(os.environ, {"DOCKER_BUILDKIT": "0"}), \
             mock.patch.object(manage, "execute") as execute:
            with self.assertRaisesRegex(manage.InfraError, "BuildKit is required"):
                manage.require_buildkit()
            execute.assert_not_called()

    def test_missing_buildx_stops_build_before_pull_or_preflight(self):
        with mock.patch.object(manage, "read_env", return_value={}), \
             mock.patch.object(manage, "validate_env"), \
             mock.patch.object(manage, "ENV_FILE") as env_file, \
             mock.patch.object(manage, "preflight") as preflight, \
             mock.patch.object(manage, "execute", side_effect=manage.InfraError("missing plugin")) as execute, \
             mock.patch.dict(os.environ, {"DOCKER_BUILDKIT": "1"}), \
             mock.patch("sys.argv", ["manage.py", "build"]):
            env_file.is_file.return_value = True
            with self.assertRaisesRegex(manage.InfraError, "docker-buildx"):
                manage.main()
            preflight.assert_not_called()
            execute.assert_called_once_with(["docker", "buildx", "version"], capture=True, timeout=20)

    def test_setup_preserves_secrets_and_cluster_id(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copy(ROOT / ".env.example", root)
            with contextlib.redirect_stdout(io.StringIO()):
                manage.setup(root)
                first = (root / ".env").read_bytes()
                manage.setup(root)
            self.assertEqual(first, (root / ".env").read_bytes())
            self.assertEqual(stat.S_IMODE((root / ".env").stat().st_mode), 0o600)
            env = manage.read_env(root / ".env")
            self.assertNotEqual(env["MONGO_PASSWORD"], env["MONGO_ROOT_PASSWORD"])
            self.assertNotIn("__GENERATE__", first.decode())
            manage.validate_env(env)

    def test_existing_invalid_env_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copy(ROOT / ".env.example", root)
            existing = "KAFKA_CLUSTER_ID=broken\n"
            (root / ".env").write_text(existing)
            with self.assertRaises(manage.InfraError):
                manage.setup(root)
            self.assertEqual(existing, (root / ".env").read_text())

    def test_conflicting_ports_and_unpinned_images_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copy(ROOT / ".env.example", root)
            with contextlib.redirect_stdout(io.StringIO()):
                manage.setup(root)
            valid = manage.read_env(root / ".env")
        for changes in ({"MONGO_HOST_PORT": valid["KAFKA_HOST_PORT"]},
                        {"MONGO_IMAGE": "mongo:latest"}, {"COMPOSE_PROJECT_NAME": "../other"},
                        {"KAFKA_CLUSTER_ID": "__GENERATE__"}):
            with self.subTest(changes=changes), self.assertRaises(manage.InfraError):
                manage.validate_env({**valid, **changes})

    def test_compose_ps_accepts_array_and_ndjson(self):
        records = [{"Service": "kafka", "State": "running"}, {"Service": "namenode", "State": "running"}]
        self.assertEqual(records, manage.parse_ps(json.dumps(records)))
        self.assertEqual(records, manage.parse_ps("\n".join(json.dumps(row) for row in records)))
        self.assertEqual([], manage.parse_ps(""))

    def test_environment_parser_preserves_literal_values(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "env"
            path.write_text('DATASET_PATH="/a path/data"\nVALUE=literal-value # comment\n')
            self.assertEqual({"DATASET_PATH": "/a path/data", "VALUE": "literal-value"}, manage.read_env(path))
            path.write_text("KEY=a\nKEY=b\n")
            with self.assertRaises(manage.InfraError):
                manage.read_env(path)

    def test_reset_without_confirmation_never_contacts_docker(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copy(ROOT / ".env.example", root)
            with contextlib.redirect_stdout(io.StringIO()):
                manage.setup(root)
            with mock.patch.object(manage, "ENV_FILE", root / ".env"), \
                 mock.patch.object(manage, "execute") as execute, \
                 mock.patch("sys.argv", ["manage.py", "reset"]), \
                 mock.patch.dict(os.environ, {"CONFIRM": ""}):
                with self.assertRaises(manage.InfraError):
                    manage.main()
                execute.assert_not_called()

    def test_project_environment_cannot_override_dotenv_or_leak_to_docker(self):
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            env_file.write_text("COMPOSE_PROJECT_NAME=expected\nMONGO_PASSWORD=from-file\n")
            with mock.patch.object(manage, "ENV_FILE", env_file), \
                 mock.patch.dict(os.environ, {"COMPOSE_PROJECT_NAME": "other-project", "DOCKER_HOST": "unix:///test.sock"}):
                child = manage.child_environment()
                self.assertNotIn("COMPOSE_PROJECT_NAME", child)
                self.assertNotIn("MONGO_PASSWORD", child)
                self.assertEqual("unix:///test.sock", child["DOCKER_HOST"])


class ComposeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        clean_env = {key: value for key, value in os.environ.items()
                     if key not in manage.read_env(ROOT / ".env.example")}
        result = subprocess.run(["docker", "compose", "--project-directory", str(ROOT),
                                 "--env-file", str(ROOT / ".env.example"),
                                 "-f", str(ROOT / "compose.yaml"), "config", "--format", "json"],
                                capture_output=True, text=True, env=clean_env, timeout=30)
        if result.returncode:
            raise AssertionError("Compose validation failed: " + result.stderr)
        cls.config = json.loads(result.stdout)
        cls.services = cls.config["services"]

    def test_complete_topology_and_memory_budget(self):
        self.assertEqual(manage.LONG_RUNNING | manage.INIT_SERVICES, set(self.services))
        total = sum(int(self.services[name]["mem_limit"]) for name in manage.LONG_RUNNING)
        self.assertLessEqual(total, 9 * 1024 ** 3)
        init_memory = sum(int(self.services[name]["mem_limit"]) for name in manage.INIT_SERVICES)
        self.assertLessEqual(total + init_memory, 10 * 1024 ** 3)
        self.assertEqual(5, len(self.config["volumes"]))

    def test_ports_are_local_and_unique(self):
        ports = []
        for service in self.services.values():
            for port in service.get("ports", []):
                self.assertEqual("127.0.0.1", port["host_ip"])
                ports.append(int(port["published"]))
        self.assertEqual(len(ports), len(set(ports)))

    def test_dependency_graph_has_no_bootstrap_deadlock(self):
        visited, stack = set(), set()

        def visit(name):
            self.assertNotIn(name, stack, "Circular service dependency")
            if name in visited:
                return
            stack.add(name)
            for dependency in self.services[name].get("depends_on", {}):
                visit(dependency)
            stack.remove(name)
            visited.add(name)

        for name in self.services:
            visit(name)
        client = self.services["spark-client"]["depends_on"]
        for name in manage.INIT_SERVICES:
            self.assertEqual("service_completed_successfully", client[name]["condition"])
        for name in ("datanode-1", "datanode-2"):
            self.assertEqual("service_healthy", self.services[name]["depends_on"]["namenode"]["condition"])
        # NN health only checks its own readiness; DataNode count belongs in hdfs-init.
        self.assertNotIn("NumLiveDataNodes", str(self.services["namenode"]["healthcheck"]))

    def test_storage_isolated_and_dataset_readonly(self):
        first = next(volume["source"] for volume in self.services["datanode-1"]["volumes"] if volume["type"] == "volume")
        second = next(volume["source"] for volume in self.services["datanode-2"]["volumes"] if volume["type"] == "volume")
        self.assertNotEqual(first, second)
        for service in self.services.values():
            for volume in service.get("volumes", []):
                if volume["type"] == "bind":
                    self.assertTrue(volume["read_only"])
                    self.assertTrue(Path(volume["source"]).exists(), volume["source"])
        dataset = next(v for v in self.services["spark-client"]["volumes"] if v["target"] == "/opt/dataset")
        self.assertFalse(dataset["bind"]["create_host_path"])

    def test_runtime_consistent_and_root_credentials_not_in_client(self):
        images = {self.services[name]["image"] for name in
                  ("spark-master", "spark-worker-1", "spark-worker-2", "spark-client")}
        self.assertEqual(1, len(images))
        self.assertNotIn("MONGO_ROOT_PASSWORD", self.services["spark-client"]["environment"])
        for name in manage.LONG_RUNNING:
            self.assertIn("healthcheck", self.services[name])


class HadoopConfigurationTests(unittest.TestCase):
    def test_replication_and_container_hostnames(self):
        tree = ET.parse(ROOT / "infra/config/hadoop/hdfs-site.xml")
        values = {entry.findtext("name"): entry.findtext("value") for entry in tree.getroot()}
        self.assertEqual("2", values["dfs.replication"])
        self.assertEqual("true", values["dfs.client.use.datanode.hostname"])
        self.assertEqual("true", values["dfs.datanode.use.datanode.hostname"])
        self.assertEqual("2", values["dfs.namenode.safemode.min.datanodes"])


if __name__ == "__main__":
    unittest.main()
