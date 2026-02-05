# transform/transform_dam_resources.py

import os
import json
import glob

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
INPUT_DIR = os.path.join(PROJECT_ROOT, "data", "input_data", "dam_resources")
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "data", "output_data", "dam_resources.json")


def transform_dam_resources(file_path: str) -> list[dict]:
    """Transform a dam resources file to match schema.sql format."""
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    dam_id = data["dam_id"]
    records = []

    # Navigate nested structure: resources.dams[0].resources[]
    dams_data = data.get("resources", {}).get("dams", [])
    if not dams_data:
        return records

    resources = dams_data[0].get("resources", [])
    for r in resources:
        records.append({
            "dam_id": dam_id,
            "date": r.get("date"),
            "storage_volume": r.get("storage_volume"),
            "percentage_full": r.get("percentage_full"),
            "storage_inflow": r.get("storage_inflow"),
            "storage_release": r.get("storage_release"),
        })

    return records


def main():
    if not os.path.exists(INPUT_DIR):
        print(f"Error: Input directory not found: {INPUT_DIR}")
        return

    input_files = glob.glob(os.path.join(INPUT_DIR, "*.json"))
    if not input_files:
        print(f"Error: No JSON files found in {INPUT_DIR}")
        return

    all_records = []
    for file_path in sorted(input_files):
        records = transform_dam_resources(file_path)
        all_records.extend(records)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2)

    print(f"Transformed {len(all_records)} resource records from {len(input_files)} dams")
    print(f"Output written to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
