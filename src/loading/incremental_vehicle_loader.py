import argparse
import hashlib
import time

from datetime import (
    datetime,
    timedelta,
    timezone,
)

from pathlib import Path

import psycopg

from google.transit import (
    gtfs_realtime_pb2
)


RAW_DIRECTORY = Path(
    "data/raw/vehicle_positions"
)

DEFAULT_LOOKBACK_MINUTES = 10
DEFAULT_BATCH_SIZE = 100


# ============================================================
# Database
# ============================================================

def get_connection():

    return psycopg.connect(
        host="localhost",
        port=5432,
        dbname="mbta",
        user="mbta",
        password="mbta",
    )


# ============================================================
# File utilities
# ============================================================

def calculate_checksum(filepath):

    sha256 = hashlib.sha256()

    with open(filepath, "rb") as file:

        while chunk := file.read(8192):

            sha256.update(chunk)

    return sha256.hexdigest()


def get_ingestion_timestamp(filepath):

    timestamp_string = (
        filepath.stem.replace(
            "vehicle_positions_",
            "",
        )
    )

    timestamp = datetime.strptime(
        timestamp_string,
        "%Y%m%dT%H%M%SZ",
    )

    return timestamp.replace(
        tzinfo=timezone.utc
    )


def unix_to_datetime(timestamp):

    if not timestamp:
        return None

    return datetime.fromtimestamp(
        timestamp,
        tz=timezone.utc,
    )


# ============================================================
# Event keys
# ============================================================

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


# ============================================================
# Snapshot parsing
# ============================================================

def parse_snapshot(filepath):

    feed = (
        gtfs_realtime_pb2.FeedMessage()
    )

    with open(filepath, "rb") as file:

        feed.ParseFromString(
            file.read()
        )

    ingestion_timestamp = (
        get_ingestion_timestamp(
            filepath
        )
    )

    records = []

    for entity in feed.entity:

        if not entity.HasField(
            "vehicle"
        ):
            continue

        vehicle = entity.vehicle

        record = {

            "entity_id":
                entity.id,

            "vehicle_id":
                vehicle.vehicle.id,

            "trip_id":
                vehicle.trip.trip_id,

            "route_id":
                vehicle.trip.route_id,

            "schedule_relationship":
                gtfs_realtime_pb2
                .TripDescriptor
                .ScheduleRelationship
                .Name(
                    vehicle
                    .trip
                    .schedule_relationship
                ),

            "direction_id":
                vehicle.trip.direction_id,

            "latitude":
                vehicle.position.latitude,

            "longitude":
                vehicle.position.longitude,

            "stop_id":
                vehicle.stop_id,

            "current_stop_sequence":
                vehicle.current_stop_sequence,

            "current_status":
                vehicle.current_status,

            "vehicle_timestamp":
                vehicle.timestamp,

            "feed_timestamp":
                feed.header.timestamp,

            "ingestion_timestamp":
                ingestion_timestamp,
        }

        records.append(record)

    return records


# ============================================================
# Incremental discovery
# ============================================================

def get_latest_processed_timestamp(
    connection,
):

    with connection.cursor() as cursor:

        cursor.execute(
            """
            SELECT MAX(
                ingestion_timestamp
            )
            FROM processed_files;
            """
        )

        return cursor.fetchone()[0]


def generate_dates(
    start_date,
    end_date,
):

    current_date = start_date

    while current_date <= end_date:

        yield current_date

        current_date += timedelta(
            days=1
        )


def discover_candidate_files(
    latest_processed_timestamp,
    lookback_minutes,
    full_scan=False,
):

    # --------------------------------------------------------
    # Recovery mode
    # --------------------------------------------------------

    if full_scan or latest_processed_timestamp is None:

        print(
            "Discovery mode: FULL SCAN"
        )

        return sorted(
            RAW_DIRECTORY.rglob("*.pb")
        )


    # --------------------------------------------------------
    # Normal incremental mode
    # --------------------------------------------------------

    print(
        "Discovery mode: INCREMENTAL"
    )


    cutoff = (
        latest_processed_timestamp
        - timedelta(
            minutes=lookback_minutes
        )
    )


    print(
        "Discovery cutoff:",
        cutoff,
    )


    candidate_files = []


    # --------------------------------------------------------
    # Look only at date partitions that could contain
    # snapshots newer than our cutoff.
    # --------------------------------------------------------

    for partition_directory in sorted(
        RAW_DIRECTORY.glob(
            "ingestion_date=*"
        )
    ):

        if not partition_directory.is_dir():
            continue


        date_string = (
            partition_directory.name
            .replace(
                "ingestion_date=",
                "",
            )
        )


        try:

            partition_date = (
                datetime.strptime(
                    date_string,
                    "%Y-%m-%d",
                ).date()
            )

        except ValueError:

            print(
                f"Skipping invalid partition: "
                f"{partition_directory}"
            )

            continue


        # Entire partition is older than the cutoff.
        if partition_date < cutoff.date():
            continue


        partition_candidates = 0


        for filepath in (
            partition_directory.glob(
                "*.pb"
            )
        ):

            try:

                file_timestamp = (
                    get_ingestion_timestamp(
                        filepath
                    )
                )


            except ValueError:

                print(
                    f"Skipping file with invalid timestamp: "
                    f"{filepath}"
                )

                continue


            # Exact file-level watermark filtering.
            if file_timestamp >= cutoff:

                candidate_files.append(
                    filepath
                )

                partition_candidates += 1


        print(
            f"{partition_directory.name}: "
            f"{partition_candidates} candidate files"
        )


    return sorted(
        candidate_files,
        key=get_ingestion_timestamp,
    )


# ============================================================
# Existing file metadata
# ============================================================

def get_processed_metadata(
    connection,
    cutoff_timestamp=None,
):

    with connection.cursor() as cursor:

        if cutoff_timestamp is None:

            cursor.execute(
                """
                SELECT
                    file_path,
                    file_checksum
                FROM processed_files;
                """
            )

        else:

            cursor.execute(
                """
                SELECT
                    file_path,
                    file_checksum

                FROM processed_files

                WHERE
                    ingestion_timestamp
                    >= %s;
                """,
                (
                    cutoff_timestamp,
                ),
            )


        rows = cursor.fetchall()


    processed_paths = {
        row[0]
        for row in rows
    }


    processed_checksums = {
        row[1]
        for row in rows
    }


    return (
        processed_paths,
        processed_checksums,
    )


# ============================================================
# Prepare one raw file
# ============================================================

def prepare_file(
    filepath,
    processed_paths,
    processed_checksums,
):

    filepath_string = str(
        filepath
    )


    # --------------------------------------------------------
    # Cheap first check:
    # if this immutable path is already known, do not even
    # open or hash the file.
    # --------------------------------------------------------

    if filepath_string in processed_paths:

        return None


    checksum = calculate_checksum(
        filepath
    )


    # --------------------------------------------------------
    # Checksum remains the authoritative duplicate safeguard.
    # --------------------------------------------------------

    if checksum in processed_checksums:

        return None


    records = parse_snapshot(
        filepath
    )


    return {
        "filepath":
            filepath,

        "filepath_string":
            filepath_string,

        "checksum":
            checksum,

        "ingestion_timestamp":
            get_ingestion_timestamp(
                filepath
            ),

        "records":
            records,
    }


# ============================================================
# Convert events to database tuples
# ============================================================

def event_to_tuple(record):

    return (

        create_event_key(
            record
        ),

        record["entity_id"],
        record["vehicle_id"],
        record["trip_id"],
        record["route_id"],
        record[
            "schedule_relationship"
        ],
        record["direction_id"],
        record["latitude"],
        record["longitude"],
        record["stop_id"],
        record[
            "current_stop_sequence"
        ],
        record["current_status"],

        unix_to_datetime(
            record[
                "vehicle_timestamp"
            ]
        ),

        unix_to_datetime(
            record[
                "feed_timestamp"
            ]
        ),

        record[
            "ingestion_timestamp"
        ],
    )


# ============================================================
# Batch loading
# ============================================================

def load_batch(
    connection,
    prepared_files,
):

    event_rows = []
    file_rows = []

    for prepared in prepared_files:

        for record in prepared["records"]:

            event_rows.append(
                event_to_tuple(record)
            )

        file_rows.append(
            (
                prepared["checksum"],
                prepared["filepath_string"],
                prepared["ingestion_timestamp"],
                len(prepared["records"]),
            )
        )

    events_seen = len(event_rows)

    try:

        with connection.cursor() as cursor:

            # ====================================================
            # 1. Create temporary staging table
            # ====================================================

            cursor.execute(
                """
                CREATE TEMP TABLE IF NOT EXISTS
                temp_vehicle_events_stage (
                    event_key TEXT,
                    entity_id TEXT,
                    vehicle_id TEXT,
                    trip_id TEXT,
                    route_id TEXT,
                    schedule_relationship TEXT,
                    direction_id INTEGER,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    stop_id TEXT,
                    current_stop_sequence INTEGER,
                    current_status INTEGER,
                    vehicle_timestamp TIMESTAMPTZ,
                    feed_timestamp TIMESTAMPTZ,
                    ingestion_timestamp TIMESTAMPTZ
                )
                ON COMMIT DELETE ROWS;
                """
            )

            # Make sure staging starts empty.
            cursor.execute(
                """
                TRUNCATE temp_vehicle_events_stage;
                """
            )

            # ====================================================
            # 2. Stage all observations from this batch
            # ====================================================

            if event_rows:

                cursor.executemany(
                    """
                    INSERT INTO temp_vehicle_events_stage (
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
                    );
                    """,
                    event_rows,
                )

                # =================================================
                # 3. Register globally unique events
                #
                # Only event_keys that are genuinely new are
                # allowed to continue into either full event table.
                # =================================================

                cursor.execute(
                    """
                    WITH candidate_events AS (

                        SELECT DISTINCT ON (event_key)
                            event_key,
                            ingestion_timestamp

                        FROM temp_vehicle_events_stage

                        ORDER BY
                            event_key,
                            ingestion_timestamp

                    ),

                    newly_registered AS (

                        INSERT INTO vehicle_event_registry (
                            event_key,
                            ingestion_timestamp
                        )

                        SELECT
                            event_key,
                            ingestion_timestamp

                        FROM candidate_events

                        ON CONFLICT (event_key)
                        DO NOTHING

                        RETURNING
                            event_key,
                            ingestion_timestamp

                    ),

                    inserted_partitioned AS (

                        INSERT INTO vehicle_events_v2 (
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

                        SELECT
                            s.event_key,
                            s.entity_id,
                            s.vehicle_id,
                            s.trip_id,
                            s.route_id,
                            s.schedule_relationship,
                            s.direction_id,
                            s.latitude,
                            s.longitude,
                            s.stop_id,
                            s.current_stop_sequence,
                            s.current_status,
                            s.vehicle_timestamp,
                            s.feed_timestamp,
                            s.ingestion_timestamp

                        FROM temp_vehicle_events_stage AS s

                        JOIN newly_registered AS n
                            ON n.event_key = s.event_key
                           AND n.ingestion_timestamp =
                               s.ingestion_timestamp

                        RETURNING event_key

                    ),

                    inserted_legacy AS (

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

                        SELECT
                            s.event_key,
                            s.entity_id,
                            s.vehicle_id,
                            s.trip_id,
                            s.route_id,
                            s.schedule_relationship,
                            s.direction_id,
                            s.latitude,
                            s.longitude,
                            s.stop_id,
                            s.current_stop_sequence,
                            s.current_status,
                            s.vehicle_timestamp,
                            s.feed_timestamp,
                            s.ingestion_timestamp

                        FROM temp_vehicle_events_stage AS s

                        JOIN newly_registered AS n
                            ON n.event_key = s.event_key
                           AND n.ingestion_timestamp =
                               s.ingestion_timestamp

                        ON CONFLICT (event_key)
                        DO NOTHING

                        RETURNING event_key

                    )

                    SELECT COUNT(*)
                    FROM inserted_partitioned;
                    """
                )

                events_inserted = (
                    cursor.fetchone()[0]
                )

            else:

                events_inserted = 0

            # ====================================================
            # 4. Mark raw files processed
            #
            # Still inside the same transaction.
            # ====================================================

            if file_rows:

                cursor.executemany(
                    """
                    INSERT INTO processed_files (
                        file_checksum,
                        file_path,
                        ingestion_timestamp,
                        event_count
                    )

                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s
                    )

                    ON CONFLICT (
                        file_checksum
                    )
                    DO NOTHING;
                    """,
                    file_rows,
                )

        connection.commit()

    except Exception:

        connection.rollback()
        raise

    return (
        events_seen,
        events_inserted,
    )


# ============================================================
# Main loader
# ============================================================

def run_incremental_load(
    lookback_minutes,
    batch_size,
    full_scan=False,
):

    loader_start = (
        time.perf_counter()
    )


    connection = (
        get_connection()
    )


    files_processed = 0
    files_skipped = 0

    total_events_seen = 0
    total_events_inserted = 0


    try:

        # ====================================================
        # 1. Read current loader watermark
        # ====================================================

        latest_processed_timestamp = (
            get_latest_processed_timestamp(
                connection
            )
        )


        print(
            "\n=== INCREMENTAL FILE DISCOVERY ==="
        )


        print(
            "Latest processed ingestion timestamp:",
            latest_processed_timestamp,
        )


        # ====================================================
        # 2. Discover only relevant raw partitions
        # ====================================================

        discovery_start = (
            time.perf_counter()
        )


        candidate_files = (
            discover_candidate_files(
                latest_processed_timestamp,
                lookback_minutes,
                full_scan=full_scan,
            )
        )


        discovery_seconds = (
            time.perf_counter()
            - discovery_start
        )


        print(
            f"Candidate files: "
            f"{len(candidate_files)}"
        )


        print(
            f"File discovery time: "
            f"{discovery_seconds:.2f} sec"
        )


        # ====================================================
        # 3. Load known metadata once
        # ====================================================

        if (
            full_scan
            or latest_processed_timestamp
            is None
        ):

            metadata_cutoff = None

        else:

            metadata_cutoff = (
                latest_processed_timestamp
                - timedelta(
                    minutes=lookback_minutes
                )
            )


        (
            processed_paths,
            processed_checksums,
        ) = get_processed_metadata(
            connection,
            metadata_cutoff,
        )


        # ====================================================
        # 4. Prepare new files and load in batches
        # ====================================================

        batch = []


        for filepath in candidate_files:

            try:

                prepared = prepare_file(
                    filepath,
                    processed_paths,
                    processed_checksums,
                )


                if prepared is None:

                    files_skipped += 1

                    continue


                batch.append(
                    prepared
                )


                # Immediately mark these values as known
                # inside this Python run so duplicate paths
                # or contents cannot enter another batch.

                processed_paths.add(
                    prepared[
                        "filepath_string"
                    ]
                )

                processed_checksums.add(
                    prepared[
                        "checksum"
                    ]
                )


                # --------------------------------------------
                # Flush batch
                # --------------------------------------------

                if len(batch) >= batch_size:

                    (
                        events_seen,
                        events_inserted,
                    ) = load_batch(
                        connection,
                        batch,
                    )


                    files_processed += (
                        len(batch)
                    )

                    total_events_seen += (
                        events_seen
                    )

                    total_events_inserted += (
                        events_inserted
                    )


                    print(
                        f"Loaded batch: "
                        f"{len(batch)} files | "
                        f"events={events_seen} | "
                        f"inserted={events_inserted}"
                    )


                    batch = []


            except Exception as error:

                connection.rollback()

                print(
                    f"FAILED: "
                    f"{filepath} | "
                    f"{error}"
                )


        # ====================================================
        # 5. Flush final partial batch
        # ====================================================

        if batch:

            (
                events_seen,
                events_inserted,
            ) = load_batch(
                connection,
                batch,
            )


            files_processed += (
                len(batch)
            )

            total_events_seen += (
                events_seen
            )

            total_events_inserted += (
                events_inserted
            )


            print(
                f"Loaded batch: "
                f"{len(batch)} files | "
                f"events={events_seen} | "
                f"inserted={events_inserted}"
            )


    finally:

        connection.close()


    total_seconds = (
        time.perf_counter()
        - loader_start
    )


    print()
    print(
        "=== LOAD SUMMARY ==="
    )

    print(
        f"Files processed: "
        f"{files_processed}"
    )

    print(
        f"Files skipped: "
        f"{files_skipped}"
    )

    print(
        f"Events seen: "
        f"{total_events_seen}"
    )

    print(
        f"Events inserted: "
        f"{total_events_inserted}"
    )

    print(
        f"Loader runtime: "
        f"{total_seconds:.2f} seconds"
    )


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "Incrementally load raw MBTA "
            "VehiclePositions snapshots."
        )
    )


    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=(
            DEFAULT_LOOKBACK_MINUTES
        ),
        help=(
            "Safety overlap used when "
            "discovering recent raw files."
        ),
    )


    parser.add_argument(
        "--batch-size",
        type=int,
        default=(
            DEFAULT_BATCH_SIZE
        ),
        help=(
            "Number of raw snapshot files "
            "loaded per database transaction."
        ),
    )


    parser.add_argument(
        "--full-scan",
        action="store_true",
        help=(
            "Recovery mode: inspect all raw "
            "snapshot partitions."
        ),
    )


    args = parser.parse_args()


    run_incremental_load(
        lookback_minutes=(
            args.lookback_minutes
        ),
        batch_size=(
            args.batch_size
        ),
        full_scan=(
            args.full_scan
        ),
    )