# scripts/db_test_queries.py

import os
import sys
import mysql.connector
from dotenv import load_dotenv
from mysql.connector import Error

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

TABLES = ["dams", "latest_data", "dam_resources", "dam_groups", "dam_group_members"]


def load_env():
    dotenv_path = os.path.join(PROJECT_ROOT, ".env")
    if not os.path.exists(dotenv_path):
        print(f"✗ Error: .env not found at {dotenv_path}")
        sys.exit(1)
    load_dotenv(dotenv_path)


def get_config():
    return dict(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
    )


def check_table_exists(cursor, table_name):
    """Check if a table exists in the database."""
    cursor.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = %s",
        (table_name,)
    )
    return cursor.fetchone()[0] > 0


def get_row_count(cursor, table_name):
    """Get the number of rows in a table."""
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cursor.fetchone()[0]


def get_latest_date(cursor, table_name, date_column="date"):
    """Get the most recent date from a table."""
    try:
        cursor.execute(f"SELECT MAX({date_column}) FROM {table_name}")
        result = cursor.fetchone()[0]
        return result if result else "No data"
    except Error:
        return "N/A"


def get_oldest_date(cursor, table_name, date_column="date"):
    """Get the oldest date from a table."""
    try:
        cursor.execute(f"SELECT MIN({date_column}) FROM {table_name}")
        result = cursor.fetchone()[0]
        return result if result else "No data"
    except Error:
        return "N/A"


def check_latest_data(cursor):
    """Check latest_data table status."""
    print("\n" + "=" * 60)
    print("latest_data Table")
    print("=" * 60)

    if not check_table_exists(cursor, "latest_data"):
        print("✗ Table does not exist")
        return False

    row_count = get_row_count(cursor, "latest_data")
    latest_date = get_latest_date(cursor, "latest_data")

    print(f"  Row count:    {row_count}")
    print(f"  Latest date:  {latest_date}")

    if row_count == 0:
        print("✗ Table is empty - run load/load_latest_data.py")
        return False

    # Check for any NULL critical fields
    cursor.execute("SELECT COUNT(*) FROM latest_data WHERE dam_id IS NULL OR date IS NULL")
    null_count = cursor.fetchone()[0]
    if null_count > 0:
        print(f"⚠ Warning: {null_count} rows with NULL dam_id or date")

    print("✓ Load successful")
    return True


def check_dam_resources(cursor):
    """Check dam_resources table status."""
    print("\n" + "=" * 60)
    print("dam_resources Table")
    print("=" * 60)

    if not check_table_exists(cursor, "dam_resources"):
        print("✗ Table does not exist")
        return False

    row_count = get_row_count(cursor, "dam_resources")
    oldest_date = get_oldest_date(cursor, "dam_resources")
    latest_date = get_latest_date(cursor, "dam_resources")

    print(f"  Row count:    {row_count}")
    print(f"  Date range:   {oldest_date} to {latest_date}")

    if row_count == 0:
        print("✗ Table is empty - run load/load_dam_resources.py")
        return False

    # Count unique dams
    cursor.execute("SELECT COUNT(DISTINCT dam_id) FROM dam_resources")
    dam_count = cursor.fetchone()[0]
    print(f"  Unique dams:  {dam_count}")

    print("✓ Load successful")
    return True


def check_dams(cursor):
    """Check dams table status."""
    print("\n" + "=" * 60)
    print("dams Table")
    print("=" * 60)

    if not check_table_exists(cursor, "dams"):
        print("✗ Table does not exist")
        return False

    row_count = get_row_count(cursor, "dams")
    print(f"  Row count:    {row_count}")

    if row_count == 0:
        print("✗ Table is empty")
        return False

    print("✓ Table has data")
    return True


def print_summary(results):
    """Print overall summary."""
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)

    all_ok = all(results.values())
    for table, status in results.items():
        icon = "✓" if status else "✗"
        print(f"  {icon} {table}")

    print()
    if all_ok:
        print("✓ All tables loaded successfully")
    else:
        print("✗ Some tables need attention")


def main():
    load_env()
    config = get_config()

    print("=" * 60)
    print("Database Table Status Check")
    print("=" * 60)
    print(f"Database: {config['database']} at {config['host']}:{config['port']}")

    try:
        conn = mysql.connector.connect(**config)
        print("✓ Connected to MySQL")
    except Error as e:
        print(f"✗ Connection error: {e}")
        sys.exit(1)

    cursor = conn.cursor(buffered=True)

    try:
        results = {
            "dams": check_dams(cursor),
            "latest_data": check_latest_data(cursor),
            "dam_resources": check_dam_resources(cursor),
        }
        print_summary(results)

    finally:
        cursor.close()
        if conn.is_connected():
            conn.close()


if __name__ == "__main__":
    main()
