import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import psycopg


OUTPUT_DIRECTORY = Path(
    "/tmp/mbta_gtfs_schedule"
)

BATCH_SIZE = 100_000


TRIP_INSTANCE_COLUMNS = [
    "feed_checksum",
    "service_date",
    "trip_id",
    "route_id",
    "service_id",
    "direction_id",
]


TRIP_INSTANCE_SCHEMA = pa.schema(
    [
        ("feed_checksum", pa.string()),
        ("service_date", pa.date32()),
        ("trip_id", pa.string()),
        ("route_id", pa.string()),
        ("service_id", pa.string()),
        ("direction_id", pa.int32()),
    ]
)


STOP_INSTANCE_COLUMNS = [
    "feed_checksum",
    "service_date",
    "trip_id",
    "stop_sequence",
    "stop_id",
    "route_id",
    "direction_id",
    "scheduled_arrival_local",
    "scheduled_departure_local",
    "arrival_seconds",
    "departure_seconds",
]


STOP_INSTANCE_SCHEMA = pa.schema(
    [
        ("feed_checksum", pa.string()),
        ("service_date", pa.date32()),
        ("trip_id", pa.string()),
        ("stop_sequence", pa.int32()),
        ("stop_id", pa.string()),
        ("route_id", pa.string()),
        ("direction_id", pa.int32()),
        (
            "scheduled_arrival_local",
            pa.timestamp("us"),
        ),
        (
            "scheduled_departure_local",
            pa.timestamp("us"),
        ),
        ("arrival_seconds", pa.int32()),
        ("departure_seconds", pa.int32()),
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


def rows_to_table(
    rows,
    columns,
    schema,
):
    records = [
        dict(zip(columns, row))
        for row in rows
    ]

    return pa.Table.from_pylist(
        records,
        schema=schema,
    )


def export_query(
    connection,
    query,
    output_path,
    columns,
    schema,
    cursor_name,
):
    writer = None
    row_count = 0

    print()
    print("================================")
    print(f"Writing: {output_path.name}")
    print("================================")

    try:
        with connection.cursor(
            name=cursor_name
        ) as cursor:

            cursor.execute(query)

            while True:

                rows = cursor.fetchmany(
                    BATCH_SIZE
                )

                if not rows:
                    break

                table = rows_to_table(
                    rows,
                    columns,
                    schema,
                )

                if writer is None:
                    writer = pq.ParquetWriter(
                        output_path,
                        schema,
                        compression="snappy",
                    )

                writer.write_table(table)

                row_count += len(rows)

                print(
                    f"{row_count:,} rows exported"
                )

    finally:

        if writer is not None:
            writer.close()

    print(
        f"COMPLETE: {row_count:,} rows"
    )

    return row_count


def main():

    OUTPUT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    trip_output = (
        OUTPUT_DIRECTORY
        / "gtfs_trip_instances.parquet"
    )

    stop_output = (
        OUTPUT_DIRECTORY
        / "gtfs_stop_instances.parquet"
    )

    with get_connection() as connection:

        trip_count = export_query(
            connection=connection,
            query="""
                SELECT
                    feed_checksum,
                    service_date,
                    trip_id,
                    route_id,
                    service_id,
                    direction_id
                FROM gtfs_trip_instances
                ORDER BY
                    service_date,
                    trip_id
            """,
            output_path=trip_output,
            columns=TRIP_INSTANCE_COLUMNS,
            schema=TRIP_INSTANCE_SCHEMA,
            cursor_name="export_gtfs_trip_instances",
        )

        stop_count = export_query(
            connection=connection,
            query="""
                SELECT
                    feed_checksum,
                    service_date,
                    trip_id,
                    stop_sequence,
                    stop_id,
                    route_id,
                    direction_id,
                    scheduled_arrival_local,
                    scheduled_departure_local,
                    arrival_seconds,
                    departure_seconds
                FROM gtfs_stop_instances
                ORDER BY
                    service_date,
                    trip_id,
                    stop_sequence
            """,
            output_path=stop_output,
            columns=STOP_INSTANCE_COLUMNS,
            schema=STOP_INSTANCE_SCHEMA,
            cursor_name="export_gtfs_stop_instances",
        )

    print()
    print("================================")
    print("GTFS SCHEDULE EXPORT COMPLETE")
    print("================================")
    print(
        f"Trip instances: {trip_count:,}"
    )
    print(
        f"Stop instances: {stop_count:,}"
    )
    print(
        f"Output: {OUTPUT_DIRECTORY}"
    )


if __name__ == "__main__":
    main()