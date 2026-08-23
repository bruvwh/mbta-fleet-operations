import json
import sys
from pathlib import Path

from google.transit import gtfs_realtime_pb2


def parse_vehicle_positions(filepath):
    feed = gtfs_realtime_pb2.FeedMessage()

    with open(filepath, "rb") as file:
        feed.ParseFromString(file.read())

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
            "bearing": vehicle.position.bearing,
            "speed": vehicle.position.speed,
            "stop_id": vehicle.stop_id,
            "current_stop_sequence": vehicle.current_stop_sequence,
            "current_status": vehicle.current_status,
            "vehicle_timestamp": vehicle.timestamp,
            "feed_timestamp": feed.header.timestamp,
        }

        records.append(record)

    return records


def save_records(records, source_filepath):
    source_path = Path(source_filepath)

    output_directory = Path("data/processed/vehicle_positions")
    output_directory.mkdir(parents=True, exist_ok=True)

    output_filename = (
        output_directory
        / f"{source_path.stem}.jsonl"
    )

    with open(output_filename, "w") as file:
        for record in records:
            file.write(json.dumps(record) + "\n")

    return output_filename


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(
            "Usage: python src/processing/normalize_vehicle_positions.py "
            "<raw_pb_file>"
        )
        sys.exit(1)

    filepath = sys.argv[1]

    records = parse_vehicle_positions(filepath)
    output_file = save_records(records, filepath)

    print(f"Parsed {len(records)} vehicle records")
    print(f"Saved to: {output_file}")