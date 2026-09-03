import argparse
import os
from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import psycopg


COLUMNS = [
    "event_key",
    "entity_id",
    "vehicle_id",
    "trip_id",
    "route_id",
    "schedule_relationship",
    "direction_id",
    "latitude",
    "longitude",
    "stop_id",
    "current_stop_sequence",
    "current_status",
    "vehicle_timestamp",
    "feed_timestamp",
    "ingestion_timestamp",
    "created_at",
]


def get_connection():
    return psycopg.connect(
        host=os.getenv("MBTA_DB_HOST", "localhost"),
        port=os.getenv("MBTA_DB_PORT", "5432"),
        dbname=os.getenv("MBTA_DB_NAME", "mbta"),
        user=os.getenv("MBTA_DB_USER", "mbta"),
        password=os.getenv("MBTA_DB_PASSWORD", "mbta"),
    )


def get_ingestion_dates(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT ingestion_timestamp::date
            FROM vehicle_events_v2
            ORDER BY 1
            """
        )

        return [row[0] for row in cursor.fetchall()]


def rows_to_table(rows):
    records = [
        dict(zip(COLUMNS, row))
        for row in rows
    ]

    return pa.Table.from_pylist(records)


def export_date(
    conn,
    ingestion_date,
    output_dir,
    batch_size,
):
    next_date = ingestion_date + timedelta(days=1)

    partition_dir = (
        output_dir
        / f"ingestion_date={ingestion_date.isoformat()}"
    )

    partition_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = partition_dir / "vehicle_events.parquet"

    print()
    print("================================")
    print(f"Exporting {ingestion_date}")
    print("================================")

    writer = None
    row_count = 0

    try:
        with conn.cursor(name=f"vehicle_export_{ingestion_date:%Y%m%d}") as cursor:

            cursor.execute(
                """
                SELECT
                    event_key,
                    entity_id,
                    vehicle_id,
                    trip_id,
                    route_id,
                    schedule_relationship,
                    direction_id,
                    latitude,
                    longitude,
                    stop_id,
                    current_stop_sequence,
                    current_status,
                    vehicle_timestamp,
                    feed_timestamp,
                    ingestion_timestamp,
                    created_at
                FROM vehicle_events_v2
                WHERE ingestion_timestamp >= %s
                  AND ingestion_timestamp < %s
                ORDER BY
                    ingestion_timestamp,
                    event_key
                """,
                (
                    ingestion_date,
                    next_date,
                ),
            )

            while True:
                rows = cursor.fetchmany(batch_size)

                if not rows:
                    break

                table = rows_to_table(rows)

                if writer is None:
                    writer = pq.ParquetWriter(
                        output_path,
                        table.schema,
                        compression="snappy",
                    )

                writer.write_table(table)

                row_count += len(rows)

                print(
                    f"{ingestion_date}: "
                    f"{row_count:,} rows exported"
                )

    finally:
        if writer is not None:
            writer.close()

    if row_count == 0:
        if output_path.exists():
            output_path.unlink()

        print(
            f"{ingestion_date}: no rows found"
        )
    else:
        print(
            f"{ingestion_date}: COMPLETE "
            f"({row_count:,} rows)"
        )

    return row_count


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--output-dir",
        default="/tmp/mbta_vehicle_events_v2",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=100_000,
    )

    args = parser.parse_args()

    output_dir = Path(args.output_dir)

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("================================")
    print("MBTA VEHICLE EVENTS EXPORT")
    print("================================")
    print(f"Output: {output_dir}")
    print(f"Batch size: {args.batch_size:,}")

    total_rows = 0

    with get_connection() as conn:

        ingestion_dates = get_ingestion_dates(conn)

        print(
            f"Ingestion dates: {len(ingestion_dates)}"
        )

        for ingestion_date in ingestion_dates:
            total_rows += export_date(
                conn=conn,
                ingestion_date=ingestion_date,
                output_dir=output_dir,
                batch_size=args.batch_size,
            )

    print()
    print("================================")
    print("EXPORT COMPLETE")
    print("================================")
    print(f"Total rows exported: {total_rows:,}")
    print(f"Output directory: {output_dir}")


if __name__ == "__main__":
    main()