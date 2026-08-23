import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import psycopg


INPUT_FILE = Path(
    "data/processed/vehicle_positions/vehicle_events.jsonl"
)


def create_event_key(record):
    key_string = (
        f"{record['vehicle_id']}|"
        f"{record['vehicle_timestamp']}|"
        f"{record['trip_id']}"
    )

    return hashlib.sha256(
        key_string.encode("utf-8")
    ).hexdigest()


def unix_to_datetime(timestamp):
    if not timestamp:
        return None

    return datetime.fromtimestamp(
        timestamp,
        tz=timezone.utc
    )


def load_records():
    records = []

    with open(INPUT_FILE, "r") as file:
        for line in file:
            records.append(json.loads(line))

    return records


def insert_records(records):
    connection = psycopg.connect(
        host="localhost",
        port=5432,
        dbname="mbta",
        user="mbta",
        password="mbta",
    )

    inserted = 0
    skipped = 0

    with connection:

        with connection.cursor() as cursor:

            for record in records:

                event_key = create_event_key(record)

                cursor.execute(
                    """
                    INSERT INTO vehicle_events (
                        event_key,
                        entity_id,
                        vehicle_id,
                        trip_id,
                        route_id,
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
                        %s, %s, %s, %s
                    )
                    ON CONFLICT (event_key) DO NOTHING
                    """,
                    (
                        event_key,
                        record["entity_id"],
                        record["vehicle_id"],
                        record["trip_id"],
                        record["route_id"],
                        record["direction_id"],
                        record["latitude"],
                        record["longitude"],
                        record["stop_id"],
                        record["current_stop_sequence"],
                        record["current_status"],
                        unix_to_datetime(
                            record["vehicle_timestamp"]
                        ),
                        unix_to_datetime(
                            record["feed_timestamp"]
                        ),
                        record["ingestion_timestamp"],
                    ),
                )

                if cursor.rowcount == 1:
                    inserted += 1
                else:
                    skipped += 1

    connection.close()

    print(f"Inserted: {inserted}")
    print(f"Skipped existing events: {skipped}")


if __name__ == "__main__":

    records = load_records()

    print(f"Loaded {len(records)} records from JSONL")

    insert_records(records)