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
    print()

    parquet_file = pq.ParquetFile(filepath)

    print("=== PARQUET SCHEMA ===")
    print(parquet_file.schema_arrow)

    print()
    print("=== METADATA ===")
    print(f"Rows: {parquet_file.metadata.num_rows}")
    print(f"Row groups: {parquet_file.metadata.num_row_groups}")
    print(f"Columns: {parquet_file.metadata.num_columns}")

    print()
    print("=== SAMPLE ROWS ===")

    table = pq.read_table(filepath)

    columns = [
        "service_date",
        "route_id",
        "trip_id",
        "stop_id",
        "stop_sequence",
        "move_timestamp",
        "stop_timestamp",
        "start_time",
        "scheduled_arrival_time",
        "scheduled_departure_time",
        "travel_time_seconds",
        "dwell_time_seconds",
    ]

    sample = (
        table.select(columns)
        .slice(0, 10)
        .to_pylist()
    )

    for row in sample:
        print(row)


if __name__ == "__main__":
    main()