# transform/transform_latest_data.py

import os
import json

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
INPUT_FILE = os.path.join(PROJECT_ROOT, "data", "input_data", "dams_resources_latest.json")
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "data", "output_data", "dams_resources_latest.json")


def transform_latest_data(item: dict) -> dict | None:
    """Transform a latest data record to match schema.sql format."""
    resources = item.get("resources", [])
    if not resources:
        return None

    r = resources[0]
    return {
        "dam_id": item["dam_id"],
        "dam_name": item["dam_name"],
        "date": r.get("date"),
        "storage_volume": r.get("storage_volume"),
        "percentage_full": r.get("percentage_full"),
        "storage_inflow": r.get("storage_inflow"),
        "storage_release": r.get("storage_release"),
    }


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file not found: {INPUT_FILE}")
        return

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        input_data = json.load(f)

    output_data = []
    for item in input_data:
        transformed = transform_latest_data(item)
        if transformed:
            output_data.append(transformed)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2)

    print(f"Transformed {len(output_data)} latest data records")
    print(f"Output written to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
