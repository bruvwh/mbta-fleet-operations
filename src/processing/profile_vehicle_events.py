import json
from collections import defaultdict
from pathlib import Path


INPUT_FILE = Path(
    "data/processed/vehicle_positions/vehicle_events.jsonl"
)


def load_records():
    records = []

    with open(INPUT_FILE, "r") as file:
        for line in file:
            records.append(json.loads(line))

    return records


def profile_records(records):
    total_records = len(records)

    missing_vehicle_id = 0
    missing_trip_id = 0
    missing_route_id = 0
    missing_vehicle_timestamp = 0

    invalid_coordinates = 0

    negative_source_latency = 0
    source_latencies = []
    latency_records = []

    timestamps_by_vehicle = defaultdict(list)

    for record in records:

        if not record["vehicle_id"]:
            missing_vehicle_id += 1

        if not record["trip_id"]:
            missing_trip_id += 1

        if not record["route_id"]:
            missing_route_id += 1

        if not record["vehicle_timestamp"]:
            missing_vehicle_timestamp += 1

        latitude = record["latitude"]
        longitude = record["longitude"]

        if not (-90 <= latitude <= 90):
            invalid_coordinates += 1

        elif not (-180 <= longitude <= 180):
            invalid_coordinates += 1

        vehicle_timestamp = record["vehicle_timestamp"]
        feed_timestamp = record["feed_timestamp"]

        if vehicle_timestamp and feed_timestamp:

            latency = feed_timestamp - vehicle_timestamp

            source_latencies.append(latency)

            latency_records.append(
                {
                    "vehicle_id": record["vehicle_id"],
                    "trip_id": record["trip_id"],
                    "route_id": record["route_id"],
                    "vehicle_timestamp": vehicle_timestamp,
                    "feed_timestamp": feed_timestamp,
                    "latency": latency,
                }
            )

            if latency < 0:
                negative_source_latency += 1

        if record["vehicle_id"] and vehicle_timestamp:
            timestamps_by_vehicle[
                record["vehicle_id"]
            ].append(vehicle_timestamp)


    out_of_order_events = 0

    for vehicle_id, timestamps in timestamps_by_vehicle.items():

        for previous, current in zip(
            timestamps,
            timestamps[1:]
        ):
            if current < previous:
                out_of_order_events += 1

    if source_latencies:
        average_latency = (
            sum(source_latencies)
            / len(source_latencies)
        )

        maximum_latency = max(source_latencies)

    else:
        average_latency = None
        maximum_latency = None

    print("=== VEHICLE EVENT DATA QUALITY REPORT ===")
    print()

    print(f"Total events: {total_records}")
    print()

    print(f"Missing vehicle IDs: {missing_vehicle_id}")
    print(f"Missing trip IDs: {missing_trip_id}")
    print(f"Missing route IDs: {missing_route_id}")
    print(
        f"Missing vehicle timestamps: "
        f"{missing_vehicle_timestamp}"
    )

    print()

    print(
        f"Invalid coordinates: "
        f"{invalid_coordinates}"
    )

    print(
        f"Negative source latency: "
        f"{negative_source_latency}"
    )

    print(
        f"Out-of-order events: "
        f"{out_of_order_events}"
    )

    print()

    print(
        f"Average source latency: "
        f"{average_latency} seconds"
    )

    print(
        f"Maximum source latency: "
        f"{maximum_latency} seconds"
    )

    latency_records.sort(
        key=lambda x: x["latency"],
        reverse=True
    )

    print("\nTop 5 highest-latency records:")

    for record in latency_records[:5]:
        print(record)


if __name__ == "__main__":

    records = load_records()

    profile_records(records)