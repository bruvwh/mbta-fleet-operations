from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pyarrow.parquet as pq


RAW_DIRECTORY = Path("data/raw/lamp_subway")


def get_connection():
    return psycopg.connect(
        host="localhost",
        port=5432,
        dbname="mbta",
        user="mbta",
        password="mbta",
    )


def parse_service_date(value):
    if value is None:
        return None

    return datetime.strptime(
        str(value),
        "%Y%m%d",
    ).date()


def unix_to_datetime(value):
    if value is None:
        return None

    return datetime.fromtimestamp(
        value,
        tz=timezone.utc,
    )


def direction_to_integer(value):
    if value is None:
        return None

    return 1 if value else 0


def get_unloaded_files(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                file_checksum,
                file_path
            FROM historical_lamp_files
            WHERE loaded_at IS NULL
            ORDER BY service_date
            """
        )

        return cursor.fetchall()

def deduplicate_rows(rows):
    grouped = {}

    for row in rows:
        key = (
            row["service_date"],
            row["trip_id"],
            row["stop_sequence"],
            row["stop_id"],
        )

        if key not in grouped:
            grouped[key] = row.copy()
            continue

        existing = grouped[key]

        # Prefer whichever record contains more populated fields.
        existing_non_null = sum(
            value is not None
            for value in existing.values()
        )

        new_non_null = sum(
            value is not None
            for value in row.values()
        )

        if new_non_null > existing_non_null:
            best = row.copy()
        else:
            best = existing.copy()

        # LAMP's canonical event logic uses the earliest
        # observed moving and stopped timestamps.
        move_timestamps = [
            value
            for value in (
                existing["move_timestamp"],
                row["move_timestamp"],
            )
            if value is not None
        ]

        stop_timestamps = [
            value
            for value in (
                existing["stop_timestamp"],
                row["stop_timestamp"],
            )
            if value is not None
        ]

        best["move_timestamp"] = (
            min(move_timestamps)
            if move_timestamps
            else None
        )

        best["stop_timestamp"] = (
            min(stop_timestamps)
            if stop_timestamps
            else None
        )

        grouped[key] = best

    return list(grouped.values())


def load_file(connection, checksum, filepath):
    print(f"\nLoading: {filepath}")

    table = pq.read_table(filepath)

    source_rows = table.to_pylist()
    rows = deduplicate_rows(source_rows)

    duplicates_collapsed = (
        len(source_rows) - len(rows)
    )

    print(f"Source rows: {len(source_rows)}")
    print(f"Canonical rows: {len(rows)}")
    print(
        f"Duplicates collapsed: "
        f"{duplicates_collapsed}"
    )

    with connection.cursor() as cursor:

        # Temporary staging table.
        # It disappears automatically after commit.
        cursor.execute(
            """
            CREATE TEMP TABLE historical_stop_events_staging
            (
                LIKE historical_stop_events
                INCLUDING DEFAULTS
            )
            ON COMMIT DROP
            """
        )

        # Bulk copy canonical rows into staging.
        with cursor.copy(
            """
            COPY historical_stop_events_staging (
                file_checksum,
                service_date,

                route_id,
                branch_route_id,
                trunk_route_id,

                trip_id,

                stop_id,
                parent_station,
                stop_sequence,

                direction_id,
                direction,
                direction_destination,

                vehicle_id,
                vehicle_label,
                vehicle_consist,

                stop_count,
                start_seconds,

                move_timestamp,
                stop_timestamp,

                travel_time_seconds,
                dwell_time_seconds,

                headway_branch_seconds,
                headway_trunk_seconds,

                scheduled_arrival_seconds,
                scheduled_departure_seconds,

                scheduled_travel_time_seconds,

                scheduled_headway_branch_seconds,
                scheduled_headway_trunk_seconds
            )
            FROM STDIN
            """
        ) as copy:

            for row in rows:

                copy.write_row(
                    (
                        checksum,

                        parse_service_date(
                            row["service_date"]
                        ),

                        row["route_id"],
                        row["branch_route_id"],
                        row["trunk_route_id"],

                        row["trip_id"],

                        row["stop_id"],
                        row["parent_station"],
                        row["stop_sequence"],

                        direction_to_integer(
                            row["direction_id"]
                        ),

                        row["direction"],
                        row["direction_destination"],

                        row["vehicle_id"],
                        row["vehicle_label"],
                        row["vehicle_consist"],

                        row["stop_count"],

                        row["start_time"],

                        unix_to_datetime(
                            row["move_timestamp"]
                        ),

                        unix_to_datetime(
                            row["stop_timestamp"]
                        ),

                        row["travel_time_seconds"],
                        row["dwell_time_seconds"],

                        row["headway_branch_seconds"],
                        row["headway_trunk_seconds"],

                        row["scheduled_arrival_time"],
                        row["scheduled_departure_time"],

                        row["scheduled_travel_time"],

                        row["scheduled_headway_branch"],
                        row["scheduled_headway_trunk"],
                    )
                )

        # Move staging rows into final table.
        cursor.execute(
            """
            INSERT INTO historical_stop_events (
                file_checksum,
                service_date,

                route_id,
                branch_route_id,
                trunk_route_id,

                trip_id,

                stop_id,
                parent_station,
                stop_sequence,

                direction_id,
                direction,
                direction_destination,

                vehicle_id,
                vehicle_label,
                vehicle_consist,

                stop_count,
                start_seconds,

                move_timestamp,
                stop_timestamp,

                travel_time_seconds,
                dwell_time_seconds,

                headway_branch_seconds,
                headway_trunk_seconds,

                scheduled_arrival_seconds,
                scheduled_departure_seconds,

                scheduled_travel_time_seconds,

                scheduled_headway_branch_seconds,
                scheduled_headway_trunk_seconds
            )

            SELECT
                file_checksum,
                service_date,

                route_id,
                branch_route_id,
                trunk_route_id,

                trip_id,

                stop_id,
                parent_station,
                stop_sequence,

                direction_id,
                direction,
                direction_destination,

                vehicle_id,
                vehicle_label,
                vehicle_consist,

                stop_count,
                start_seconds,

                move_timestamp,
                stop_timestamp,

                travel_time_seconds,
                dwell_time_seconds,

                headway_branch_seconds,
                headway_trunk_seconds,

                scheduled_arrival_seconds,
                scheduled_departure_seconds,

                scheduled_travel_time_seconds,

                scheduled_headway_branch_seconds,
                scheduled_headway_trunk_seconds

            FROM historical_stop_events_staging

            ON CONFLICT (
                service_date,
                trip_id,
                stop_sequence,
                stop_id
            )
            DO NOTHING
            """
        )

        inserted = cursor.rowcount

        cursor.execute(
            """
            UPDATE historical_lamp_files

            SET
                loaded_at = NOW(),
                row_count = %s

            WHERE file_checksum = %s
            """,
            (
                len(rows),
                checksum,
            ),
        )

    connection.commit()

    print(f"Rows inserted: {inserted}")


def run_loader():
    connection = get_connection()

    try:

        files = get_unloaded_files(connection)

        print(
            f"Historical files waiting to load: "
            f"{len(files)}"
        )

        for checksum, filepath in files:

            try:
                load_file(
                    connection,
                    checksum,
                    Path(filepath),
                )

            except Exception as error:
                connection.rollback()

                print(
                    f"FAILED: {filepath} | "
                    f"{error}"
                )

                raise

    finally:
        connection.close()


if __name__ == "__main__":
    run_loader()