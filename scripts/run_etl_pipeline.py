# scripts/run_etl_pipeline.py

"""
Master script to run the full ETL pipeline with tests at each stage.

Usage:
    python scripts/run_etl_pipeline.py           # Run full pipeline
    python scripts/run_etl_pipeline.py --no-tests  # Skip tests
"""

import os
import sys
import subprocess
import argparse
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Script definitions for each stage
EXTRACT_SCRIPTS = [
    "extract/api_calls/fetch_token.py",
    "extract/api_calls/fetch_dam_resources_latest.py",
    "extract/api_calls/fetch_dam_resources.py",
]

TRANSFORM_SCRIPTS = [
    "transform/transform_dam_resources_latest.py",
    "transform/transform_dam_resources.py",
]

LOAD_SCRIPTS = [
    "load/load_latest_data.py",
    "load/load_dam_resources.py",
]

TEST_FILES = {
    "extract": "tests/test_extract.py",
    "transform": "tests/test_transform.py",
    "load": "tests/test_load.py",
}


def print_header(title: str):
    """Print a formatted header."""
    print("\n" + "=" * 70)
    print(f" {title}")
    print("=" * 70)


def print_subheader(title: str):
    """Print a formatted subheader."""
    print(f"\n--- {title} ---")


def run_script(script_path: str) -> bool:
    """Run a Python script and return success status."""
    full_path = os.path.join(PROJECT_ROOT, script_path)

    if not os.path.exists(full_path):
        print(f"✗ Script not found: {script_path}")
        return False

    print(f"\nRunning: {script_path}")
    result = subprocess.run(
        [sys.executable, full_path],
        cwd=PROJECT_ROOT
    )

    if result.returncode != 0:
        print(f"✗ {script_path} failed with exit code {result.returncode}")
        return False

    return True


def run_tests(test_file: str) -> bool:
    """Run pytest on a test file and return success status."""
    full_path = os.path.join(PROJECT_ROOT, test_file)

    if not os.path.exists(full_path):
        print(f"✗ Test file not found: {test_file}")
        return False

    print(f"\nRunning tests: {test_file}")
    result = subprocess.run(
        [sys.executable, "-m", "pytest", full_path, "-v", "--tb=short"],
        cwd=PROJECT_ROOT
    )

    if result.returncode != 0:
        print(f"✗ Tests failed: {test_file}")
        return False

    print(f"✓ Tests passed: {test_file}")
    return True


def run_stage(stage_name: str, scripts: list, test_file: str, run_tests_flag: bool) -> bool:
    """Run all scripts for a stage and optionally run tests."""
    print_header(f"{stage_name.upper()} STAGE")

    # Run scripts
    for script in scripts:
        if not run_script(script):
            print(f"\n✗ {stage_name.upper()} STAGE FAILED")
            return False

    print(f"\n✓ All {stage_name} scripts completed")

    # Run tests if enabled
    if run_tests_flag:
        print_subheader(f"{stage_name.capitalize()} Tests")
        if not run_tests(test_file):
            print(f"\n✗ {stage_name.upper()} TESTS FAILED")
            return False

    print(f"\n✓ {stage_name.upper()} STAGE COMPLETE")
    return True


def main():
    parser = argparse.ArgumentParser(description="Run the full ETL pipeline")
    parser.add_argument(
        "--no-tests",
        action="store_true",
        help="Skip running tests after each stage"
    )
    parser.add_argument(
        "--stage",
        choices=["extract", "transform", "load"],
        help="Run only a specific stage"
    )
    args = parser.parse_args()

    run_tests_flag = not args.no_tests
    start_time = datetime.now()

    print_header("ETL PIPELINE")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Tests: {'enabled' if run_tests_flag else 'disabled'}")

    stages = [
        ("extract", EXTRACT_SCRIPTS, TEST_FILES["extract"]),
        ("transform", TRANSFORM_SCRIPTS, TEST_FILES["transform"]),
        ("load", LOAD_SCRIPTS, TEST_FILES["load"]),
    ]

    # Filter to specific stage if requested
    if args.stage:
        stages = [(name, scripts, tests) for name, scripts, tests in stages if name == args.stage]

    # Run stages
    for stage_name, scripts, test_file in stages:
        if not run_stage(stage_name, scripts, test_file, run_tests_flag):
            print_header("PIPELINE FAILED")
            print(f"Failed at: {stage_name} stage")
            sys.exit(1)

    # Summary
    end_time = datetime.now()
    duration = end_time - start_time

    print_header("PIPELINE COMPLETE")
    print(f"Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")
    print("\n✓ All stages completed successfully")


if __name__ == "__main__":
    main()
