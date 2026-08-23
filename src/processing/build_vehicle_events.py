import json
from datetime import datetime, timezone
from pathlib import Path

from google.transit import gtfs_realtime_pb2


RAW_DIRECTORY = Path("data/raw/vehicle_positions")
OUTPUT_FILE = Path(
    "data/processed/vehicle_positions/vehicle_events.jsonl"
)


def get_ingestion_timestamp(filepath):
    timestamp_string = filepath.stem.replace(
        "vehicle_positions_", ""
    )

    timestamp = datetime.strptime(
        timestamp_string,
        "%Y%m%dT%H%M%SZ",
    )

    return timestamp.replace(
        tzinfo=timezone.utc
    ).isoformat()


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


def build_event_dataset():
    raw_files = sorted(
        RAW_DIRECTORY.rglob("*.pb")
    )

    print(f"Found {len(raw_files)} raw snapshots")

    seen_events = set()
    unique_records = []

    total_records = 0
    duplicate_records = 0

    for filepath in raw_files:
        records = parse_snapshot(filepath)

        for record in records:
            total_records += 1

            event_key = (
                record["vehicle_id"],
                record["vehicle_timestamp"],
                record["trip_id"],
            )

            if event_key in seen_events:
                duplicate_records += 1
                continue

            seen_events.add(event_key)
            unique_records.append(record)

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(OUTPUT_FILE, "w") as file:
        for record in unique_records:
            file.write(json.dumps(record) + "\n")

    print(f"Total records: {total_records}")
    print(f"Duplicate records: {duplicate_records}")
    print(f"Unique events: {len(unique_records)}")
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    build_event_dataset()