# tests/test_transform.py

import os
import json
import pytest
from pydantic import ValidationError

from schemas import LatestDataRecord, DamResourceRecord

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "output_data")
LATEST_DATA_FILE = os.path.join(OUTPUT_DATA_DIR, "dams_resources_latest.json")
DAM_RESOURCES_FILE = os.path.join(OUTPUT_DATA_DIR, "dam_resources.json")


class TestTransformLatestData:
    """Tests for transform_dam_resources_latest.py output."""

    def test_output_file_exists(self):
        """Verify the transformed latest data file exists."""
        assert os.path.exists(LATEST_DATA_FILE), (
            f"Output file not found: {LATEST_DATA_FILE}\n"
            "Run: python transform/transform_dam_resources_latest.py"
        )

    def test_output_file_is_valid_json(self):
        """Verify the output file contains valid JSON."""
        if not os.path.exists(LATEST_DATA_FILE):
            pytest.skip("Output file does not exist")

        with open(LATEST_DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert isinstance(data, list), "Expected a list of records"

    def test_output_has_records(self):
        """Verify the output file contains records."""
        if not os.path.exists(LATEST_DATA_FILE):
            pytest.skip("Output file does not exist")

        with open(LATEST_DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert len(data) > 0, "Expected at least one record"

    def test_all_records_match_schema(self):
        """Verify all records match the LatestDataRecord schema."""
        if not os.path.exists(LATEST_DATA_FILE):
            pytest.skip("Output file does not exist")

        with open(LATEST_DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        errors = []
        for i, record in enumerate(data):
            try:
                LatestDataRecord(**record)
            except ValidationError as e:
                errors.append(f"Record {i} (dam_id={record.get('dam_id')}): {e}")

        assert not errors, f"Schema validation errors:\n" + "\n".join(errors)

    def test_dam_ids_are_unique(self):
        """Verify all dam_ids are unique in latest data."""
        if not os.path.exists(LATEST_DATA_FILE):
            pytest.skip("Output file does not exist")

        with open(LATEST_DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        dam_ids = [record["dam_id"] for record in data]
        assert len(dam_ids) == len(set(dam_ids)), "Duplicate dam_ids found"

    def test_dates_are_valid(self):
        """Verify all dates can be parsed."""
        if not os.path.exists(LATEST_DATA_FILE):
            pytest.skip("Output file does not exist")

        with open(LATEST_DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        for record in data:
            validated = LatestDataRecord(**record)
            assert validated.date is not None, f"Invalid date for dam_id={record['dam_id']}"


class TestTransformDamResources:
    """Tests for transform_dam_resources.py output."""

    def test_output_file_exists(self):
        """Verify the transformed dam resources file exists."""
        assert os.path.exists(DAM_RESOURCES_FILE), (
            f"Output file not found: {DAM_RESOURCES_FILE}\n"
            "Run: python transform/transform_dam_resources.py"
        )

    def test_output_file_is_valid_json(self):
        """Verify the output file contains valid JSON."""
        if not os.path.exists(DAM_RESOURCES_FILE):
            pytest.skip("Output file does not exist")

        with open(DAM_RESOURCES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert isinstance(data, list), "Expected a list of records"

    def test_output_has_records(self):
        """Verify the output file contains records."""
        if not os.path.exists(DAM_RESOURCES_FILE):
            pytest.skip("Output file does not exist")

        with open(DAM_RESOURCES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert len(data) > 0, "Expected at least one record"

    def test_all_records_match_schema(self):
        """Verify all records match the DamResourceRecord schema."""
        if not os.path.exists(DAM_RESOURCES_FILE):
            pytest.skip("Output file does not exist")

        with open(DAM_RESOURCES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        errors = []
        for i, record in enumerate(data):
            try:
                DamResourceRecord(**record)
            except ValidationError as e:
                errors.append(f"Record {i} (dam_id={record.get('dam_id')}): {e}")

        assert not errors, f"Schema validation errors:\n" + "\n".join(errors)

    def test_has_multiple_dams(self):
        """Verify data from multiple dams is present."""
        if not os.path.exists(DAM_RESOURCES_FILE):
            pytest.skip("Output file does not exist")

        with open(DAM_RESOURCES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        unique_dams = set(record["dam_id"] for record in data)
        assert len(unique_dams) > 1, "Expected data from multiple dams"

    def test_dates_are_valid(self):
        """Verify all dates can be parsed."""
        if not os.path.exists(DAM_RESOURCES_FILE):
            pytest.skip("Output file does not exist")

        with open(DAM_RESOURCES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        for record in data:
            validated = DamResourceRecord(**record)
            assert validated.date is not None, f"Invalid date for dam_id={record['dam_id']}"

    def test_no_duplicate_dam_date_pairs(self):
        """Verify no duplicate (dam_id, date) pairs exist."""
        if not os.path.exists(DAM_RESOURCES_FILE):
            pytest.skip("Output file does not exist")

        with open(DAM_RESOURCES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        pairs = [(record["dam_id"], record["date"]) for record in data]
        assert len(pairs) == len(set(pairs)), "Duplicate (dam_id, date) pairs found"
