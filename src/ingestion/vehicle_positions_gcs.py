import argparse
import time

from datetime import (
    datetime,
    timezone,
)

import requests

from google.cloud import storage


# ============================================================
# Configuration
# ============================================================

MBTA_VEHICLE_POSITIONS_URL = (
    "https://cdn.mbta.com/realtime/"
    "VehiclePositions.pb"
)

PROJECT_ID = (
    "mbta-fleet-operations-alu"
)

BUCKET_NAME = (
    "mbta-fleet-operations-alu"
)

GCS_BASE_PREFIX = (
    "raw/vehicle_positions"
)

DEFAULT_POLL_SECONDS = 5

REQUEST_TIMEOUT_SECONDS = 20


# ============================================================
# Arguments
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Collect MBTA GTFS-Realtime "
            "VehiclePositions directly to GCS."
        )
    )

    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=DEFAULT_POLL_SECONDS,
        help=(
            "Seconds between requests. "
            "Default: 5."
        ),
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help=(
            "Collect one snapshot and exit. "
            "Useful for testing."
        ),
    )

    parser.add_argument(
        "--max-snapshots",
        type=int,
        default=None,
        help=(
            "Optional maximum number of snapshots "
            "to collect before exiting."
        ),
    )

    return parser.parse_args()


# ============================================================
# GCS
# ============================================================

def get_bucket():

    client = storage.Client(
        project=PROJECT_ID
    )

    return client.bucket(
        BUCKET_NAME
    )


# ============================================================
# MBTA fetch
# ============================================================

def fetch_snapshot():

    response = requests.get(
        MBTA_VEHICLE_POSITIONS_URL,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    response.raise_for_status()

    if not response.content:

        raise RuntimeError(
            "MBTA returned an empty "
            "VehiclePositions response."
        )

    return response.content


# ============================================================
# Object naming
# ============================================================

def build_object_name(
    ingestion_timestamp,
):

    ingestion_date = (
        ingestion_timestamp
        .date()
        .isoformat()
    )

    timestamp_string = (
        ingestion_timestamp
        .strftime(
            "%Y%m%dT%H%M%SZ"
        )
    )

    filename = (
        "vehicle_positions_"
        f"{timestamp_string}.pb"
    )

    return (
        f"{GCS_BASE_PREFIX}/"
        f"ingestion_date="
        f"{ingestion_date}/"
        f"{filename}"
    )


# ============================================================
# Upload
# ============================================================

def upload_snapshot(
    bucket,
    snapshot_bytes,
    ingestion_timestamp,
):

    object_name = (
        build_object_name(
            ingestion_timestamp
        )
    )

    blob = bucket.blob(
        object_name
    )

    # Raw snapshots are immutable.
    #
    # if_generation_match=0 means:
    # only create the object if it does
    # not already exist.
    blob.upload_from_string(
        snapshot_bytes,
        content_type=(
            "application/octet-stream"
        ),
        if_generation_match=0,
    )

    return object_name


# ============================================================
# Collector
# ============================================================

def collect():

    args = parse_args()

    if args.poll_seconds <= 0:

        raise ValueError(
            "--poll-seconds must be > 0."
        )

    if (
        args.max_snapshots is not None
        and args.max_snapshots <= 0
    ):

        raise ValueError(
            "--max-snapshots must be > 0."
        )

    bucket = get_bucket()

    collected_count = 0

    print(
        "======================================"
    )

    print(
        "MBTA VEHICLE POSITIONS GCS COLLECTOR"
    )

    print(
        "======================================"
    )

    print(
        f"Source: {MBTA_VEHICLE_POSITIONS_URL}"
    )

    print(
        f"Bucket: gs://{BUCKET_NAME}"
    )

    print(
        f"Prefix: {GCS_BASE_PREFIX}"
    )

    print(
        f"Poll interval: "
        f"{args.poll_seconds} seconds"
    )

    if args.once:

        print(
            "Mode: one snapshot"
        )

    elif args.max_snapshots:

        print(
            "Maximum snapshots:",
            args.max_snapshots,
        )

    else:

        print(
            "Mode: continuous"
        )

    print()

    while True:

        cycle_start = (
            time.perf_counter()
        )

        try:

            snapshot_bytes = (
                fetch_snapshot()
            )

            ingestion_timestamp = (
                datetime.now(
                    timezone.utc
                )
            )

            object_name = (
                upload_snapshot(
                    bucket,
                    snapshot_bytes,
                    ingestion_timestamp,
                )
            )

            collected_count += 1

            print(
                f"[{ingestion_timestamp.isoformat()}] "
                f"uploaded "
                f"{len(snapshot_bytes):,} bytes"
            )

            print(
                f"gs://{BUCKET_NAME}/"
                f"{object_name}"
            )

        except Exception as error:

            print(
                f"Collection failed: "
                f"{type(error).__name__}: "
                f"{error}"
            )

        # --------------------------------------------
        # Test modes
        # --------------------------------------------

        if args.once:

            break

        if (
            args.max_snapshots
            is not None

            and collected_count
            >= args.max_snapshots
        ):

            break

        # --------------------------------------------
        # Maintain approximate polling interval
        # --------------------------------------------

        elapsed = (
            time.perf_counter()
            - cycle_start
        )

        sleep_seconds = max(
            0,
            args.poll_seconds
            - elapsed,
        )

        time.sleep(
            sleep_seconds
        )

    print()

    print(
        "Collector finished."
    )

    print(
        f"Snapshots uploaded: "
        f"{collected_count}"
    )


if __name__ == "__main__":

    collect()