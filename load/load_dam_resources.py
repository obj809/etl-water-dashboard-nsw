# load/load_dam_resources.py

import os
import json
import mysql.connector
from dotenv import load_dotenv

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
INPUT_FILE = os.path.join(PROJECT_ROOT, "data", "output_data", "dam_resources.json")

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


def get_existing_dates(cursor, dam_id: str) -> set:
    """Get all existing dates for a dam_id to avoid duplicates."""
    cursor.execute(
        "SELECT date FROM dam_resources WHERE dam_id = %s",
        (dam_id,)
    )
    return {row[0] for row in cursor.fetchall()}


def load_dam_resources(records: list[dict]):
    """
    Append new records to dam_resources table.
    Skips records that already exist (same dam_id + date).
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Group records by dam_id for efficient duplicate checking
        records_by_dam = {}
        for record in records:
            dam_id = record["dam_id"]
            if dam_id not in records_by_dam:
                records_by_dam[dam_id] = []
            records_by_dam[dam_id].append(record)

        insert_sql = """
            INSERT INTO dam_resources
            (dam_id, date, storage_volume, percentage_full, storage_inflow, storage_release)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        inserted = 0
        skipped = 0

        for dam_id, dam_records in records_by_dam.items():
            existing_dates = get_existing_dates(cursor, dam_id)

            for record in dam_records:
                record_date = record["date"]
                # Convert string date to date object for comparison if needed
                if isinstance(record_date, str):
                    from datetime import datetime
                    record_date_obj = datetime.strptime(record_date, "%Y-%m-%d").date()
                else:
                    record_date_obj = record_date

                if record_date_obj in existing_dates:
                    skipped += 1
                    continue

                cursor.execute(insert_sql, (
                    record["dam_id"],
                    record["date"],
                    record.get("storage_volume"),
                    record.get("percentage_full"),
                    record.get("storage_inflow"),
                    record.get("storage_release"),
                ))
                inserted += 1

        conn.commit()
        print(f"✓ Inserted {inserted} new records into dam_resources")
        if skipped > 0:
            print(f"  Skipped {skipped} existing records (already in database)")

    except mysql.connector.Error as e:
        conn.rollback()
        raise RuntimeError(f"Database error: {e}")
    finally:
        cursor.close()
        conn.close()


def main():
    print("=" * 60)
    print("Load Dam Resources to Database")
    print("=" * 60)

    if not os.path.exists(INPUT_FILE):
        print(f"✗ Error: Input file not found: {INPUT_FILE}")
        print("Run transform/transform_dam_resources.py first.")
        exit(1)

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        records = json.load(f)

    print(f"Found {len(records)} records to process")

    try:
        load_dam_resources(records)
        print("=" * 60)
        print("✓ COMPLETE")
        print("=" * 60)
    except RuntimeError as e:
        print(f"\n✗ ERROR: {e}\n")
        exit(1)


if __name__ == "__main__":
    main()
