# load/load_latest_data.py

import os
import sys
import json

# Add scripts directory to path for imports
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))

from db_utils import DatabaseConnection

INPUT_FILE = os.path.join(PROJECT_ROOT, "data", "output_data", "dams_resources_latest.json")


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
    with DatabaseConnection() as db:
        cursor = db.get_cursor()

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

            db.commit()
            print(f"✓ Inserted {inserted} records into latest_data")

        except Exception as e:
            db.rollback()
            raise RuntimeError(f"Database error: {e}")
        finally:
            cursor.close()


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
