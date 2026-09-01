import hashlib
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from google.transit import gtfs_realtime_pb2


RAW_DIRECTORY = Path("data/raw/vehicle_positions")


def calculate_checksum(filepath):
    sha256 = hashlib.sha256()

    with open(filepath, "rb") as file:
        while chunk := file.read(8192):
            sha256.update(chunk)

    return sha256.hexdigest()


def get_ingestion_timestamp(filepath):
    timestamp_string = filepath.stem.replace(
        "vehicle_positions_", ""
    )

    timestamp = datetime.strptime(
        timestamp_string,
        "%Y%m%dT%H%M%SZ",
    )

    return timestamp.replace(tzinfo=timezone.utc)


def unix_to_datetime(timestamp):
    if not timestamp:
        return None

    return datetime.fromtimestamp(
        timestamp,
        tz=timezone.utc,
    )


def create_event_key(record):
    if record["vehicle_timestamp"]:
        key_string = (
            f"{record['vehicle_id']}|"
            f"{record['vehicle_timestamp']}|"
            f"{record['trip_id']}"
        )
    else:
        key_string = (
            f"{record['vehicle_id']}|"
            f"{record['feed_timestamp']}|"
            f"{record['trip_id']}|"
            f"{record['entity_id']}"
        )

    return hashlib.sha256(
        key_string.encode("utf-8")
    ).hexdigest()


def parse_snapshot(filepath):
    feed = gtfs_realtime_pb2.FeedMessage()

    with open(filepath, "rb") as file:
        feed.ParseFromString(file.read())

    ingestion_timestamp = get_ingestion_timestamp(filepath)

    records = []

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue

        vehicle = entity.vehicle

        record = {
            "entity_id": entity.id,
            "vehicle_id": vehicle.vehicle.id,
            "trip_id": vehicle.trip.trip_id,
            "route_id": vehicle.trip.route_id,
            "schedule_relationship": (
                gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(
                    vehicle.trip.schedule_relationship
                )
            ),
            "direction_id": vehicle.trip.direction_id,
            "latitude": vehicle.position.latitude,
            "longitude": vehicle.position.longitude,
            "stop_id": vehicle.stop_id,
            "current_stop_sequence": vehicle.current_stop_sequence,
            "current_status": vehicle.current_status,
            "vehicle_timestamp": vehicle.timestamp,
            "feed_timestamp": feed.header.timestamp,
            "ingestion_timestamp": ingestion_timestamp,
        }

        records.append(record)

    return records


def file_already_processed(cursor, checksum):
    cursor.execute(
        """
        SELECT 1
        FROM processed_files
        WHERE file_checksum = %s
        """,
        (checksum,),
    )

    return cursor.fetchone() is not None


def insert_event(cursor, record):
    event_key = create_event_key(record)

    cursor.execute(
        """
        INSERT INTO vehicle_events (
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
            ingestion_timestamp
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (event_key) DO NOTHING
        """,
        (
            event_key,
            record["entity_id"],
            record["vehicle_id"],
            record["trip_id"],
            record["route_id"],
            record["schedule_relationship"],
            record["direction_id"],
            record["latitude"],
            record["longitude"],
            record["stop_id"],
            record["current_stop_sequence"],
            record["current_status"],
            unix_to_datetime(record["vehicle_timestamp"]),
            unix_to_datetime(record["feed_timestamp"]),
            record["ingestion_timestamp"],
        ),
    )

    return cursor.rowcount


def process_file(connection, filepath):
    checksum = calculate_checksum(filepath)

    with connection.cursor() as cursor:

        if file_already_processed(cursor, checksum):
            return "already_processed", 0, 0

        records = parse_snapshot(filepath)

        inserted = 0

        for record in records:
            inserted += insert_event(cursor, record)

        cursor.execute(
            """
            INSERT INTO processed_files (
                file_checksum,
                file_path,
                ingestion_timestamp,
                event_count
            )
            VALUES (%s, %s, %s, %s)
            """,
            (
                checksum,
                str(filepath),
                get_ingestion_timestamp(filepath),
                len(records),
            ),
        )

    connection.commit()

    return "processed", len(records), inserted


def run_incremental_load():
    raw_files = sorted(
        RAW_DIRECTORY.rglob("*.pb")
    )

    print(f"Found {len(raw_files)} raw files")

    connection = psycopg.connect(
        host="localhost",
        port=5432,
        dbname="mbta",
        user="mbta",
        password="mbta",
    )

    files_processed = 0
    files_skipped = 0
    total_events_seen = 0
    total_events_inserted = 0

    try:
        for filepath in raw_files:

            try:
                status, events_seen, events_inserted = (
                    process_file(connection, filepath)
                )

                if status == "already_processed":
                    files_skipped += 1
                    print(f"Skipped: {filepath}")

                else:
                    files_processed += 1
                    total_events_seen += events_seen
                    total_events_inserted += events_inserted

                    print(
                        f"Processed: {filepath} | "
                        f"events={events_seen} | "
                        f"inserted={events_inserted}"
                    )

            except Exception as error:
                connection.rollback()

                print(
                    f"FAILED: {filepath} | "
                    f"{error}"
                )

    finally:
        connection.close()

    print()
    print("=== LOAD SUMMARY ===")
    print(f"Files processed: {files_processed}")
    print(f"Files skipped: {files_skipped}")
    print(f"Events seen: {total_events_seen}")
    print(f"Events inserted: {total_events_inserted}")


if __name__ == "__main__":
    run_incremental_load()