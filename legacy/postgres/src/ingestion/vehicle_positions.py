from datetime import datetime, timezone
from pathlib import Path
import time

import requests


URL = "https://cdn.mbta.com/realtime/VehiclePositions.pb"


def download_feed():
    response = requests.get(URL, timeout=30)
    response.raise_for_status()

    return response.content


def save_raw_feed(data):
    now = datetime.now(timezone.utc)

    directory = (
        Path("data/raw/vehicle_positions")
        / f"ingestion_date={now:%Y-%m-%d}"
    )

    directory.mkdir(parents=True, exist_ok=True)

    filename = (
        directory
        / f"vehicle_positions_{now:%Y%m%dT%H%M%SZ}.pb"
    )

    filename.write_bytes(data)

    return filename


def collect(interval_seconds=5):
    print("Starting MBTA vehicle position collector")
    print(f"Polling every {interval_seconds} seconds")
    print("Press Ctrl+C to stop\n")

    while True:
        try:
            data = download_feed()

            filepath = save_raw_feed(data)

            print(f"Saved: {filepath}")

            time.sleep(interval_seconds)

        except requests.RequestException as error:
            print(f"Request failed: {error}")
            print("Trying again in 30 seconds...")

            time.sleep(30)


if __name__ == "__main__":
    collect()