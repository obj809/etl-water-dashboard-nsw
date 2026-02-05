# scripts/db_seed_dam_resources.py

import os
import sys
import subprocess
from dotenv import load_dotenv

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOAD_SCRIPT = "load/load_dam_resources.py"


def main() -> None:
    env_path = os.path.join(PROJECT_ROOT, ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)

    db = os.getenv("DB_NAME", "(unknown)")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "3306")
    print(f"Target DB: {db} at {host}:{port}")

    full_path = os.path.join(PROJECT_ROOT, LOAD_SCRIPT)
    if not os.path.isfile(full_path):
        print(f"✗ Missing: {LOAD_SCRIPT}")
        sys.exit(1)

    print(f"\n{'=' * 60}")
    print(f"Running {LOAD_SCRIPT}")
    print("=" * 60)
    result = subprocess.run([sys.executable, full_path], cwd=PROJECT_ROOT)
    if result.returncode != 0:
        print(f"✗ {LOAD_SCRIPT} failed with exit code {result.returncode}")
        sys.exit(result.returncode)

    print("\n" + "=" * 60)
    print("✓ Dam resources load completed successfully.")
    print("=" * 60)


if __name__ == "__main__":
    main()
