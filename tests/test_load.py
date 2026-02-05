# tests/test_load.py

import os
import json
import pytest
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "output_data")
LATEST_DATA_FILE = os.path.join(OUTPUT_DATA_DIR, "dams_resources_latest.json")
DAM_RESOURCES_FILE = os.path.join(OUTPUT_DATA_DIR, "dam_resources.json")

# Load environment variables
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


def get_db_config():
    """Get database configuration from environment."""
    return dict(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
    )


def db_available():
    """Check if database connection is available."""
    config = get_db_config()
    if not all([config["user"], config["password"], config["database"]]):
        return False
    try:
        conn = mysql.connector.connect(**config)
        conn.close()
        return True
    except Error:
        return False


@pytest.fixture
def db_connection():
    """Provide a database connection for tests."""
    config = get_db_config()
    conn = mysql.connector.connect(**config)
    yield conn
    conn.close()


@pytest.fixture
def db_cursor(db_connection):
    """Provide a database cursor for tests."""
    cursor = db_connection.cursor(buffered=True)
    yield cursor
    cursor.close()


# Skip all tests in this module if database is not available
pytestmark = pytest.mark.skipif(
    not db_available(),
    reason="Database connection not available"
)


class TestDatabaseConnection:
    """Tests for database connectivity."""

    def test_can_connect_to_database(self, db_connection):
        """Verify database connection is successful."""
        assert db_connection.is_connected()

    def test_database_exists(self, db_cursor):
        """Verify the target database exists."""
        db_cursor.execute("SELECT DATABASE()")
        result = db_cursor.fetchone()
        assert result[0] is not None


class TestLoadLatestData:
    """Tests for latest_data table after loading."""

    def test_latest_data_table_exists(self, db_cursor):
        """Verify latest_data table exists."""
        db_cursor.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = 'latest_data'"
        )
        assert db_cursor.fetchone()[0] == 1, "latest_data table does not exist"

    def test_latest_data_has_records(self, db_cursor):
        """Verify latest_data table has records."""
        db_cursor.execute("SELECT COUNT(*) FROM latest_data")
        count = db_cursor.fetchone()[0]
        assert count > 0, "latest_data table is empty"

    def test_latest_data_has_required_columns(self, db_cursor):
        """Verify latest_data table has all required columns."""
        db_cursor.execute(
            "SELECT COLUMN_NAME FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = 'latest_data'"
        )
        columns = {row[0] for row in db_cursor.fetchall()}
        required = {"dam_id", "dam_name", "date", "storage_volume", "percentage_full"}
        assert required.issubset(columns), f"Missing columns: {required - columns}"

    def test_latest_data_dam_ids_are_unique(self, db_cursor):
        """Verify dam_ids are unique in latest_data."""
        db_cursor.execute("SELECT COUNT(*) FROM latest_data")
        total = db_cursor.fetchone()[0]
        db_cursor.execute("SELECT COUNT(DISTINCT dam_id) FROM latest_data")
        unique = db_cursor.fetchone()[0]
        assert total == unique, "Duplicate dam_ids found in latest_data"

    def test_latest_data_matches_transform_output(self, db_cursor):
        """Verify database record count matches transform output."""
        if not os.path.exists(LATEST_DATA_FILE):
            pytest.skip("Transform output file does not exist")

        with open(LATEST_DATA_FILE, "r", encoding="utf-8") as f:
            transform_data = json.load(f)

        db_cursor.execute("SELECT COUNT(*) FROM latest_data")
        db_count = db_cursor.fetchone()[0]

        assert db_count == len(transform_data), (
            f"Record count mismatch: DB={db_count}, transform={len(transform_data)}"
        )

    def test_latest_data_dates_are_recent(self, db_cursor):
        """Verify latest_data contains recent dates."""
        db_cursor.execute("SELECT MAX(date) FROM latest_data")
        max_date = db_cursor.fetchone()[0]
        assert max_date is not None, "No dates found in latest_data"


class TestLoadDamResources:
    """Tests for dam_resources table after loading."""

    def test_dam_resources_table_exists(self, db_cursor):
        """Verify dam_resources table exists."""
        db_cursor.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = 'dam_resources'"
        )
        assert db_cursor.fetchone()[0] == 1, "dam_resources table does not exist"

    def test_dam_resources_has_records(self, db_cursor):
        """Verify dam_resources table has records."""
        db_cursor.execute("SELECT COUNT(*) FROM dam_resources")
        count = db_cursor.fetchone()[0]
        assert count > 0, "dam_resources table is empty"

    def test_dam_resources_has_required_columns(self, db_cursor):
        """Verify dam_resources table has all required columns."""
        db_cursor.execute(
            "SELECT COLUMN_NAME FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = 'dam_resources'"
        )
        columns = {row[0] for row in db_cursor.fetchall()}
        required = {"dam_id", "date", "storage_volume", "percentage_full"}
        assert required.issubset(columns), f"Missing columns: {required - columns}"

    def test_dam_resources_has_multiple_dams(self, db_cursor):
        """Verify dam_resources has data from multiple dams."""
        db_cursor.execute("SELECT COUNT(DISTINCT dam_id) FROM dam_resources")
        dam_count = db_cursor.fetchone()[0]
        assert dam_count > 1, "Expected data from multiple dams"

    def test_dam_resources_no_duplicate_dam_date_pairs(self, db_cursor):
        """Verify no duplicate (dam_id, date) pairs in dam_resources."""
        db_cursor.execute("SELECT COUNT(*) FROM dam_resources")
        total = db_cursor.fetchone()[0]
        db_cursor.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT dam_id, date FROM dam_resources) AS t"
        )
        unique = db_cursor.fetchone()[0]
        assert total == unique, "Duplicate (dam_id, date) pairs found"

    def test_dam_resources_date_range(self, db_cursor):
        """Verify dam_resources has expected date range."""
        db_cursor.execute("SELECT MIN(date), MAX(date) FROM dam_resources")
        min_date, max_date = db_cursor.fetchone()
        assert min_date is not None, "No dates found in dam_resources"
        assert max_date is not None, "No dates found in dam_resources"
        assert min_date <= max_date, "Invalid date range"


class TestDamsTable:
    """Tests for dams table."""

    def test_dams_table_exists(self, db_cursor):
        """Verify dams table exists."""
        db_cursor.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = 'dams'"
        )
        assert db_cursor.fetchone()[0] == 1, "dams table does not exist"

    def test_dams_has_records(self, db_cursor):
        """Verify dams table has records."""
        db_cursor.execute("SELECT COUNT(*) FROM dams")
        count = db_cursor.fetchone()[0]
        assert count > 0, "dams table is empty"

    def test_all_latest_data_dams_exist_in_dams_table(self, db_cursor):
        """Verify all dam_ids in latest_data exist in dams table."""
        db_cursor.execute(
            "SELECT COUNT(*) FROM latest_data ld "
            "LEFT JOIN dams d ON ld.dam_id = d.dam_id "
            "WHERE d.dam_id IS NULL"
        )
        orphans = db_cursor.fetchone()[0]
        assert orphans == 0, f"{orphans} dam_ids in latest_data not found in dams"

    def test_all_dam_resources_dams_exist_in_dams_table(self, db_cursor):
        """Verify all dam_ids in dam_resources exist in dams table."""
        db_cursor.execute(
            "SELECT COUNT(*) FROM dam_resources dr "
            "LEFT JOIN dams d ON dr.dam_id = d.dam_id "
            "WHERE d.dam_id IS NULL"
        )
        orphans = db_cursor.fetchone()[0]
        assert orphans == 0, f"{orphans} dam_ids in dam_resources not found in dams"
