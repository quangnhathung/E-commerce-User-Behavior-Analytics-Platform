"""Local API bootstrap and configuration require no web container."""
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('serving_host_tests',ROOT/'services/manage.py')
host=importlib.util.module_from_spec(spec)
spec.loader.exec_module(host)


class ServingHostTests(unittest.TestCase):
    def test_dotenv_app_credentials_override_shell_without_passing_root_account(self):
        values={'MONGO_USERNAME':'app-user','MONGO_PASSWORD':'fixture-password','MONGO_DATABASE':'ecommerce',
                'MONGO_HOST_PORT':'27028','API_HOST_PORT':'8001'}
        with mock.patch.object(host,'read_env',return_value=values),mock.patch.object(Path,'exists',return_value=True), \
             mock.patch.dict(os.environ,{'MONGO_PASSWORD':'shell-value','MONGO_ROOT_PASSWORD':'root-value'}):
            env=host.environment()
        self.assertEqual('fixture-password',env['MONGO_PASSWORD'])
        self.assertNotIn('MONGO_ROOT_PASSWORD',env)
        self.assertEqual('127.0.0.1',env['MONGO_HOST'])
        self.assertEqual('27028',env['MONGO_PORT'])

    def test_static_web_tests_do_not_require_api_or_dotenv(self):
        with mock.patch('sys.argv',['manage.py','test-web']),mock.patch.object(host.shutil,'which',return_value='/node'), \
             mock.patch.object(host,'execute') as execute,mock.patch.object(host,'environment') as env:
            host.main()
        env.assert_not_called()
        self.assertEqual('/node',execute.call_args.args[0][0])

    def test_api_binds_only_to_loopback(self):
        with mock.patch('sys.argv',['manage.py','api']),mock.patch.object(Path,'exists',return_value=True), \
             mock.patch.object(host,'environment',return_value={'API_HOST_PORT':'8000'}),mock.patch.object(host,'execute') as execute:
            host.main()
        command=execute.call_args.args[0]
        self.assertEqual('127.0.0.1',command[command.index('--host')+1])
        self.assertNotIn('docker',command)

    def test_failed_smoke_cannot_keep_a_previous_passed_result(self):
        class Process:
            stdout=['SERVING_RESULT={"passed": true}\n']
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def wait(self):return 1
        with tempfile.TemporaryDirectory() as directory,mock.patch.object(host,'ARTIFACTS',Path(directory)), \
             mock.patch.object(host,'ROOT',Path(directory)),mock.patch.object(host.subprocess,'Popen',return_value=Process()):
            with self.assertRaises(RuntimeError):host.recorded(['fixture'],{},'smoke')
            report=json.loads((Path(directory)/'smoke.json').read_text())
            self.assertFalse(report['passed'])
            self.assertEqual(1,report['exit_code'])
