import importlib.util
from pathlib import Path
import tempfile
import unittest
import zipfile

from helpers import APP


class PackagingTests(unittest.TestCase):
    def test_archive_is_reproducible_and_contains_only_python_package(self):
        spec = importlib.util.spec_from_file_location("package_code", APP / "package_code.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            first = module.package_code(Path(directory) / "first.zip")
            second = module.package_code(Path(directory) / "second.zip")
            self.assertEqual(first["sha256"], second["sha256"])
            with zipfile.ZipFile(first["archive"]) as archive:
                self.assertIn("ecommerce_analytics/contracts.py", archive.namelist())
                self.assertTrue(all(name.startswith("ecommerce_analytics/") and name.endswith(".py")
                                    for name in archive.namelist()))


if __name__ == "__main__":
    unittest.main()
