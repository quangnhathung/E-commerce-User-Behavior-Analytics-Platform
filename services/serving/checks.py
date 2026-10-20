"""Emit a machine-readable result only after the complete selected suite exits."""
import argparse
import json
import sys
import unittest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("suite", choices=["api"])
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    tests = unittest.defaultTestLoader.discover("tests", pattern=f"test_{args.suite}*.py")
    if args.offline:
        tests = unittest.defaultTestLoader.loadTestsFromName("test_api.APITests")
    result = unittest.TextTestRunner(stream=sys.stdout, verbosity=2).run(tests)
    passed = result.wasSuccessful() and result.testsRun > 0 and not result.skipped
    print("SERVING_RESULT=" + json.dumps({"passed": passed, "tests_run": result.testsRun,
        "skipped": len(result.skipped), "errors": len(result.errors), "failures": len(result.failures)}))
    sys.exit(not passed)
