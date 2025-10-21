"""Small test runner that sets PYTHONPATH to `src` and runs pytest for unit-only tests.

Run locally with: python scripts/run_unit_tests.py
This avoids collecting integration tests that require external services.
"""
import os
import sys
import subprocess


def main() -> int:
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    src_path = os.path.join(root, "src")
    # Prepend src to PYTHONPATH for import resolution
    env = os.environ.copy()
    env["PYTHONPATH"] = src_path + (os.pathsep + env.get("PYTHONPATH", ""))

    # Run pytest only on the src/test directory
    cmd = [sys.executable, "-m", "pytest", "-q", "src/test"]
    print("Running:", " ".join(cmd))
    return subprocess.call(cmd, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
