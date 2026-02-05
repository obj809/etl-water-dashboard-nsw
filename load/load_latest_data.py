# load/load_latest_data.py

import os
import json
import mysql.connector
from dotenv import load_dotenv

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
INPUT_FILE = os.path.join(PROJECT_ROOT, "data", "output_data", "dams_resources_latest.json")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_NAME]):
    raise RuntimeError("DB_USER, DB_PASSWORD, and DB_NAME must be set in .env file")


def get_connection():
    """Create and return a database connection."""
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )


def ensure_dams_exist(cursor, records: list[dict]):
    """
    Ensure all dams in records exist in the dams table.
    Inserts missing dams to satisfy foreign key constraint.
    """
    # Get existing dam_ids
    cursor.execute("SELECT dam_id FROM dams")
    existing_dams = {row[0] for row in cursor.fetchall()}

    # Find and insert missing dams
    dams_to_insert = {}
    for record in records:
        dam_id = record["dam_id"]
        if dam_id not in existing_dams and dam_id not in dams_to_insert:
            dams_to_insert[dam_id] = record["dam_name"]

    if dams_to_insert:
        insert_dam_sql = """
            INSERT INTO dams (dam_id, dam_name)
            VALUES (%s, %s)
        """
        for dam_id, dam_name in dams_to_insert.items():
            cursor.execute(insert_dam_sql, (dam_id, dam_name))
        print(f"✓ Added {len(dams_to_insert)} new dams to dams table")


def load_latest_data(records: list[dict]):
    """
    Replace all data in the latest_data table with new records.
    Ensures dams exist in dams table first (foreign key constraint).
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Ensure all dams exist in dams table
        ensure_dams_exist(cursor, records)

        # Clear existing data
        cursor.execute("DELETE FROM latest_data")
        print(f"✓ Cleared existing latest_data records")

        # Insert new records
        insert_sql = """
            INSERT INTO latest_data
            (dam_id, dam_name, date, storage_volume, percentage_full, storage_inflow, storage_release)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        inserted = 0
        for record in records:
            cursor.execute(insert_sql, (
                record["dam_id"],
                record["dam_name"],
                record["date"],
                record.get("storage_volume"),
                record.get("percentage_full"),
                record.get("storage_inflow"),
                record.get("storage_release"),
            ))
            inserted += 1

        conn.commit()
        print(f"✓ Inserted {inserted} records into latest_data")

    except mysql.connector.Error as e:
        conn.rollback()
        raise RuntimeError(f"Database error: {e}")
    finally:
        cursor.close()
        conn.close()


def main():
    print("=" * 60)
    print("Load Latest Data to Database")
    print("=" * 60)

    if not os.path.exists(INPUT_FILE):
        print(f"✗ Error: Input file not found: {INPUT_FILE}")
        print("Run transform/transform_dam_resources_latest.py first.")
        exit(1)

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        records = json.load(f)

    print(f"Found {len(records)} records to load")

    try:
        load_latest_data(records)
        print("=" * 60)
        print("✓ COMPLETE")
        print("=" * 60)
    except RuntimeError as e:
        print(f"\n✗ ERROR: {e}\n")
        exit(1)


if __name__ == "__main__":
    main()
