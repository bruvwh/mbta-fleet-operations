import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import psycopg


OUTPUT_DIRECTORY = Path(
    "/tmp/mbta_vehicle_event_schedule_matches"
)

OUTPUT_PATH = (
    OUTPUT_DIRECTORY
    / "vehicle_event_schedule_matches.parquet"
)

BATCH_SIZE = 100_000


COLUMNS = [
    "event_key",
    "event_ingestion_timestamp",
    "feed_checksum",
    "service_date",
    "trip_id",
    "stop_sequence",
    "scheduled_local_time",
    "difference_seconds",
    "created_at",
]


SCHEMA = pa.schema(
    [
        ("event_key", pa.string()),
        (
            "event_ingestion_timestamp",
            pa.timestamp("us", tz="UTC"),
        ),
        ("feed_checksum", pa.string()),
        ("service_date", pa.date32()),
        ("trip_id", pa.string()),
        ("stop_sequence", pa.int32()),
        (
            "scheduled_local_time",
            pa.timestamp("us"),
        ),
        ("difference_seconds", pa.float64()),
        (
            "created_at",
            pa.timestamp("us", tz="UTC"),
        ),
    ]
)


def get_connection():
    return psycopg.connect(
        host=os.getenv(
            "MBTA_DB_HOST",
            "localhost",
        ),
        port=os.getenv(
            "MBTA_DB_PORT",
            "5432",
        ),
        dbname=os.getenv(
            "MBTA_DB_NAME",
            "mbta",
        ),
        user=os.getenv(
            "MBTA_DB_USER",
            "mbta",
        ),
        password=os.getenv(
            "MBTA_DB_PASSWORD",
            "mbta",
        ),
    )


def rows_to_table(rows):
    records = [
        dict(zip(COLUMNS, row))
        for row in rows
    ]

    return pa.Table.from_pylist(
        records,
        schema=SCHEMA,
    )


def main():

    OUTPUT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    if OUTPUT_PATH.exists():
        OUTPUT_PATH.unlink()

    print("================================")
    print("VEHICLE EVENT SCHEDULE MATCH EXPORT")
    print("================================")
    print(f"Output: {OUTPUT_PATH}")
    print(f"Batch size: {BATCH_SIZE:,}")

    writer = None
    row_count = 0

    try:

        with get_connection() as connection:

            with connection.cursor(
                name="export_vehicle_event_schedule_matches"
            ) as cursor:

                cursor.execute(
                    """
                    SELECT
                        event_key,
                        event_ingestion_timestamp,
                        feed_checksum,
                        service_date,
                        trip_id,
                        stop_sequence,
                        scheduled_local_time,
                        difference_seconds,
                        created_at

                    FROM vehicle_event_schedule_matches

                    ORDER BY
                        event_ingestion_timestamp,
                        event_key
                    """
                )

                while True:

                    rows = cursor.fetchmany(
                        BATCH_SIZE
                    )

                    if not rows:
                        break

                    table = rows_to_table(
                        rows
                    )

                    if writer is None:

                        writer = pq.ParquetWriter(
                            OUTPUT_PATH,
                            SCHEMA,
                            compression="snappy",
                        )

                    writer.write_table(
                        table
                    )

                    row_count += len(rows)

                    print(
                        f"{row_count:,} rows exported"
                    )

    finally:

        if writer is not None:
            writer.close()

    print()
    print("================================")
    print("EXPORT COMPLETE")
    print("================================")
    print(
        f"Rows exported: {row_count:,}"
    )
    print(
        f"Output: {OUTPUT_PATH}"
    )


if __name__ == "__main__":
    main()