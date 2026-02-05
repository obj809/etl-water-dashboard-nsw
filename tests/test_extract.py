# tests/test_extract.py

import os
import json
import glob
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
INPUT_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "input_data")
DAM_RESOURCES_DIR = os.path.join(INPUT_DATA_DIR, "dam_resources")
LATEST_DATA_FILE = os.path.join(INPUT_DATA_DIR, "dams_resources_latest.json")


class TestExtractDamResourcesLatest:
    """Tests for fetch_dam_resources_latest.py output."""

    def test_output_file_exists(self):
        """Verify the latest data output file exists."""
        assert os.path.exists(LATEST_DATA_FILE), (
            f"Output file not found: {LATEST_DATA_FILE}\n"
            "Run: python extract/api_calls/fetch_dam_resources_latest.py"
        )

    def test_output_file_is_valid_json(self):
        """Verify the output file contains valid JSON."""
        if not os.path.exists(LATEST_DATA_FILE):
            pytest.skip("Output file does not exist")

        with open(LATEST_DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert isinstance(data, list), "Expected a list of dam records"

    def test_output_has_records(self):
        """Verify the output file contains dam records."""
        if not os.path.exists(LATEST_DATA_FILE):
            pytest.skip("Output file does not exist")

        with open(LATEST_DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert len(data) > 0, "Expected at least one dam record"

    def test_records_have_required_fields(self):
        """Verify each record has required fields."""
        if not os.path.exists(LATEST_DATA_FILE):
            pytest.skip("Output file does not exist")

        with open(LATEST_DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        required_fields = ["dam_id", "dam_name", "resources"]

        for record in data:
            for field in required_fields:
                assert field in record, f"Missing field '{field}' in record"

    def test_resources_have_expected_structure(self):
        """Verify resources contain expected data fields."""
        if not os.path.exists(LATEST_DATA_FILE):
            pytest.skip("Output file does not exist")

        with open(LATEST_DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        resource_fields = ["date", "storage_volume", "percentage_full"]

        for record in data:
            resources = record.get("resources", [])
            if resources:
                resource = resources[0]
                for field in resource_fields:
                    assert field in resource, (
                        f"Missing field '{field}' in resources for dam {record.get('dam_id')}"
                    )


class TestExtractDamResources:
    """Tests for fetch_dam_resources.py output."""

    def test_output_directory_exists(self):
        """Verify the dam resources output directory exists."""
        assert os.path.exists(DAM_RESOURCES_DIR), (
            f"Output directory not found: {DAM_RESOURCES_DIR}\n"
            "Run: python extract/api_calls/fetch_dam_resources.py"
        )

    def test_output_directory_has_files(self):
        """Verify the output directory contains JSON files."""
        if not os.path.exists(DAM_RESOURCES_DIR):
            pytest.skip("Output directory does not exist")

        json_files = glob.glob(os.path.join(DAM_RESOURCES_DIR, "*.json"))
        assert len(json_files) > 0, "Expected at least one JSON file in dam_resources"

    def test_output_files_are_valid_json(self):
        """Verify all output files contain valid JSON."""
        if not os.path.exists(DAM_RESOURCES_DIR):
            pytest.skip("Output directory does not exist")

        json_files = glob.glob(os.path.join(DAM_RESOURCES_DIR, "*.json"))
        if not json_files:
            pytest.skip("No JSON files found")

        for file_path in json_files:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            assert isinstance(data, dict), f"Expected dict in {file_path}"

    def test_files_have_required_fields(self):
        """Verify each file has required fields."""
        if not os.path.exists(DAM_RESOURCES_DIR):
            pytest.skip("Output directory does not exist")

        json_files = glob.glob(os.path.join(DAM_RESOURCES_DIR, "*.json"))
        if not json_files:
            pytest.skip("No JSON files found")

        required_fields = ["dam_id", "dam_name", "start_date", "end_date", "resources"]

        for file_path in json_files:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            for field in required_fields:
                assert field in data, f"Missing field '{field}' in {file_path}"

    def test_dam_id_matches_filename(self):
        """Verify dam_id in file matches the filename."""
        if not os.path.exists(DAM_RESOURCES_DIR):
            pytest.skip("Output directory does not exist")

        json_files = glob.glob(os.path.join(DAM_RESOURCES_DIR, "*.json"))
        if not json_files:
            pytest.skip("No JSON files found")

        for file_path in json_files:
            filename = os.path.basename(file_path)
            expected_dam_id = filename.replace(".json", "")

            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            assert data.get("dam_id") == expected_dam_id, (
                f"dam_id mismatch: file={filename}, dam_id={data.get('dam_id')}"
            )


class TestExtractDataIntegrity:
    """Cross-file data integrity tests."""

    def test_latest_and_resources_have_matching_dams(self):
        """Verify dams in latest data also have resource files."""
        if not os.path.exists(LATEST_DATA_FILE):
            pytest.skip("Latest data file does not exist")
        if not os.path.exists(DAM_RESOURCES_DIR):
            pytest.skip("Dam resources directory does not exist")

        with open(LATEST_DATA_FILE, "r", encoding="utf-8") as f:
            latest_data = json.load(f)

        latest_dam_ids = {record["dam_id"] for record in latest_data}

        resource_files = glob.glob(os.path.join(DAM_RESOURCES_DIR, "*.json"))
        resource_dam_ids = {
            os.path.basename(f).replace(".json", "") for f in resource_files
        }

        # Check overlap (not exact match due to excluded dams and API errors)
        common_dams = latest_dam_ids & resource_dam_ids
        assert len(common_dams) > 0, "Expected some dams to appear in both datasets"
