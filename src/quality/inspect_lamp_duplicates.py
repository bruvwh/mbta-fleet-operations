from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq


RAW_DIRECTORY = Path("data/raw/lamp_subway")


def main():
    files = sorted(
        RAW_DIRECTORY.rglob("*.parquet")
    )

    if not files:
        print("No LAMP parquet files found.")
        return

    filepath = files[0]

    print(f"Inspecting: {filepath}")

    table = pq.read_table(
        filepath,
        columns=[
            "service_date",
            "trip_id",
            "stop_sequence",
            "stop_id",
            "vehicle_id",
            "start_time",
            "move_timestamp",
            "stop_timestamp",
        ],
    )

    rows = table.to_pylist()

    keys = [
        (
            row["service_date"],
            row["trip_id"],
            row["stop_sequence"],
            row["stop_id"],
        )
        for row in rows
    ]

    counts = Counter(keys)

    duplicate_keys = {
        key: count
        for key, count in counts.items()
        if count > 1
    }

    print()
    print("=== DUPLICATE PRIMARY KEYS ===")
    print(f"Duplicate keys: {len(duplicate_keys)}")

    for key, count in duplicate_keys.items():
        print()
        print(f"Key: {key}")
        print(f"Occurrences: {count}")

        for row in rows:
            row_key = (
                row["service_date"],
                row["trip_id"],
                row["stop_sequence"],
                row["stop_id"],
            )

            if row_key == key:
                print(row)


if __name__ == "__main__":
    main()