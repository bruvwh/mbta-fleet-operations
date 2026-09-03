import argparse
import hashlib

from datetime import (
    date,
    datetime,
    time,
    timedelta,
    timezone,
)

from google.cloud import (
    bigquery,
    storage,
)

from google.transit import (
    gtfs_realtime_pb2,
)


# ============================================================
# Configuration
# ============================================================

PROJECT_ID = (
    "mbta-fleet-operations-alu"
)

DATASET_ID = (
    "mbta_analytics"
)

BUCKET_NAME = (
    "mbta-fleet-operations-alu"
)

VEHICLE_PREFIX = (
    "raw/vehicle_positions"
)


TARGET_TABLE = (
    f"{PROJECT_ID}."
    f"{DATASET_ID}."
    "vehicle_events"
)

PROCESSED_TABLE = (
    f"{PROJECT_ID}."
    f"{DATASET_ID}."
    "realtime_processed_files"
)

EVENT_STAGE_TABLE = (
    f"{PROJECT_ID}."
    f"{DATASET_ID}."
    "_vehicle_events_stage"
)

FILE_STAGE_TABLE = (
    f"{PROJECT_ID}."
    f"{DATASET_ID}."
    "_vehicle_files_stage"
)


DEFAULT_LOOKBACK_MINUTES = 30

DEFAULT_MAX_FILES = 500


# ============================================================
# BigQuery schemas
# ============================================================

EVENT_SCHEMA = [

    bigquery.SchemaField(
        "event_key",
        "STRING",
        mode="REQUIRED",
    ),

    bigquery.SchemaField(
        "entity_id",
        "STRING",
    ),

    bigquery.SchemaField(
        "vehicle_id",
        "STRING",
    ),

    bigquery.SchemaField(
        "trip_id",
        "STRING",
    ),

    bigquery.SchemaField(
        "route_id",
        "STRING",
    ),

    bigquery.SchemaField(
        "schedule_relationship",
        "STRING",
    ),

    bigquery.SchemaField(
        "direction_id",
        "INTEGER",
    ),

    bigquery.SchemaField(
        "latitude",
        "FLOAT",
    ),

    bigquery.SchemaField(
        "longitude",
        "FLOAT",
    ),

    bigquery.SchemaField(
        "stop_id",
        "STRING",
    ),

    bigquery.SchemaField(
        "current_stop_sequence",
        "INTEGER",
    ),

    bigquery.SchemaField(
        "current_status",
        "INTEGER",
    ),

    bigquery.SchemaField(
        "vehicle_timestamp",
        "TIMESTAMP",
    ),

    bigquery.SchemaField(
        "feed_timestamp",
        "TIMESTAMP",
    ),

    bigquery.SchemaField(
        "ingestion_timestamp",
        "TIMESTAMP",
        mode="REQUIRED",
    ),
]


FILE_SCHEMA = [

    bigquery.SchemaField(
        "object_name",
        "STRING",
        mode="REQUIRED",
    ),

    bigquery.SchemaField(
        "ingestion_timestamp",
        "TIMESTAMP",
        mode="REQUIRED",
    ),

    bigquery.SchemaField(
        "event_count",
        "INTEGER",
        mode="REQUIRED",
    ),
]


# ============================================================
# Timestamp utilities
# ============================================================

def unix_to_datetime(
    timestamp,
):

    if not timestamp:

        return None

    return datetime.fromtimestamp(
        timestamp,
        tz=timezone.utc,
    )


def get_object_ingestion_timestamp(
    object_name,
):

    filename = (
        object_name
        .rsplit("/", 1)[-1]
    )

    timestamp_string = (
        filename
        .replace(
            "vehicle_positions_",
            "",
        )
        .replace(
            ".pb",
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


# ============================================================
# Event key
# ============================================================

def create_event_key(
    record,
):

    if record[
        "vehicle_timestamp_raw"
    ]:

        key_string = (
            f"{record['vehicle_id']}|"
            f"{record['vehicle_timestamp_raw']}|"
            f"{record['trip_id']}"
        )

    else:

        key_string = (
            f"{record['vehicle_id']}|"
            f"{record['feed_timestamp_raw']}|"
            f"{record['trip_id']}|"
            f"{record['entity_id']}"
        )

    return hashlib.sha256(
        key_string.encode(
            "utf-8"
        )
    ).hexdigest()


# ============================================================
# Parse one protobuf object
# ============================================================

def parse_snapshot(
    data,
    ingestion_timestamp,
):

    feed = (
        gtfs_realtime_pb2
        .FeedMessage()
    )

    feed.ParseFromString(
        data
    )


    feed_timestamp_raw = (
        feed.header.timestamp
    )


    rows = []


    for entity in feed.entity:

        if not entity.HasField(
            "vehicle"
        ):

            continue


        vehicle = entity.vehicle


        if vehicle.HasField(
            "position"
        ):

            latitude = (
                vehicle.position.latitude
            )

            longitude = (
                vehicle.position.longitude
            )

        else:

            latitude = None
            longitude = None


        vehicle_timestamp_raw = (
            vehicle.timestamp
        )


        record = {

            "entity_id":
                entity.id
                or None,

            "vehicle_id":
                vehicle.vehicle.id
                or None,

            "trip_id":
                vehicle.trip.trip_id
                or None,

            "route_id":
                vehicle.trip.route_id
                or None,

            "schedule_relationship":
                (
                    gtfs_realtime_pb2
                    .TripDescriptor
                    .ScheduleRelationship
                    .Name(
                        vehicle
                        .trip
                        .schedule_relationship
                    )
                ),

            "direction_id":
                int(
                    vehicle.trip.direction_id
                ),

            "latitude":
                latitude,

            "longitude":
                longitude,

            "stop_id":
                vehicle.stop_id
                or None,

            "current_stop_sequence":
                (
                    int(
                        vehicle
                        .current_stop_sequence
                    )

                    if (
                        vehicle
                        .current_stop_sequence
                    )

                    else None
                ),

            "current_status":
                int(
                    vehicle.current_status
                ),

            "vehicle_timestamp_raw":
                vehicle_timestamp_raw,

            "feed_timestamp_raw":
                feed_timestamp_raw,

            "vehicle_timestamp":
                unix_to_datetime(
                    vehicle_timestamp_raw
                ),

            "feed_timestamp":
                unix_to_datetime(
                    feed_timestamp_raw
                ),

            "ingestion_timestamp":
                ingestion_timestamp,
        }


        record[
            "event_key"
        ] = create_event_key(
            record
        )


        record.pop(
            "vehicle_timestamp_raw"
        )

        record.pop(
            "feed_timestamp_raw"
        )


        rows.append(
            record
        )


    return rows


# ============================================================
# Processed-file registry
# ============================================================

def create_processed_table(
    client,
):

    query = f"""
    CREATE TABLE IF NOT EXISTS
        `{PROCESSED_TABLE}`
    (
        object_name STRING NOT NULL,

        ingestion_timestamp
            TIMESTAMP NOT NULL,

        processed_at
            TIMESTAMP NOT NULL,

        event_count
            INT64 NOT NULL
    )

    PARTITION BY
        DATE(ingestion_timestamp)

    CLUSTER BY
        object_name
    """

    client.query(
        query
    ).result()


def get_latest_processed_timestamp(
    client,
):

    query = f"""
    SELECT
        MAX(
            ingestion_timestamp
        ) AS latest_timestamp

    FROM
        `{PROCESSED_TABLE}`
    """

    rows = list(
        client.query(
            query
        ).result()
    )

    if not rows:

        return None

    return rows[0][
        "latest_timestamp"
    ]


def get_processed_objects(
    client,
    cutoff=None,
):

    if cutoff is None:

        query = f"""
        SELECT
            object_name

        FROM
            `{PROCESSED_TABLE}`
        """

        job_config = None

    else:

        query = f"""
        SELECT
            object_name

        FROM
            `{PROCESSED_TABLE}`

        WHERE
            ingestion_timestamp
                >= @cutoff
        """

        job_config = (
            bigquery.QueryJobConfig(
                query_parameters=[

                    bigquery
                    .ScalarQueryParameter(
                        "cutoff",
                        "TIMESTAMP",
                        cutoff,
                    )

                ]
            )
        )


    rows = (
        client.query(
            query,
            job_config=job_config,
        )
        .result()
    )


    return {
        row[
            "object_name"
        ]

        for row in rows
    }


# ============================================================
# GCS discovery
# ============================================================

def date_range(
    start_date,
    end_date,
):

    current_date = (
        start_date
    )

    while (
        current_date
        <= end_date
    ):

        yield current_date

        current_date += (
            timedelta(
                days=1
            )
        )


def discover_candidate_blobs(
    storage_client,
    bigquery_client,
    lookback_minutes,
    max_files,
    full_scan=False,
):

    bucket = (
        storage_client.bucket(
            BUCKET_NAME
        )
    )


    latest_processed = (
        get_latest_processed_timestamp(
            bigquery_client
        )
    )


    if (
        full_scan

        or latest_processed
        is None
    ):

        cutoff = None

        blobs = list(
            storage_client.list_blobs(
                BUCKET_NAME,

                prefix=(
                    f"{VEHICLE_PREFIX}/"
                ),
            )
        )

    else:

        cutoff = (
            latest_processed
            - timedelta(
                minutes=(
                    lookback_minutes
                )
            )
        )


        now_utc = (
            datetime.now(
                timezone.utc
            )
        )


        blobs = []


        for partition_date in (
            date_range(
                cutoff.date(),
                now_utc.date(),
            )
        ):

            partition_prefix = (

                f"{VEHICLE_PREFIX}/"
                f"ingestion_date="
                f"{partition_date.isoformat()}/"
            )


            blobs.extend(

                storage_client
                .list_blobs(
                    BUCKET_NAME,
                    prefix=(
                        partition_prefix
                    ),
                )

            )


    processed_objects = (
        get_processed_objects(
            bigquery_client,
            cutoff=cutoff,
        )
    )


    candidates = []


    for blob in blobs:

        if not blob.name.endswith(
            ".pb"
        ):

            continue


        if blob.name in (
            processed_objects
        ):

            continue


        try:

            ingestion_timestamp = (
                get_object_ingestion_timestamp(
                    blob.name
                )
            )

        except ValueError:

            print(
                "Skipping object with "
                "invalid timestamp:",
                blob.name,
            )

            continue


        if (
            cutoff is not None

            and ingestion_timestamp
            < cutoff
        ):

            continue


        candidates.append(
            (
                ingestion_timestamp,
                blob,
            )
        )


    candidates.sort(
        key=lambda item:
            item[0]
    )


    if max_files:

        candidates = (
            candidates[
                :max_files
            ]
        )


    return candidates


# ============================================================
# JSON serialization utilities
# ============================================================


def make_json_safe(
    value,
):

    if isinstance(
        value,
        (
            datetime,
            date,
            time,
        ),
    ):

        return value.isoformat()

    if isinstance(
        value,
        dict,
    ):

        return {
            key: make_json_safe(
                nested_value
            )

            for (
                key,
                nested_value,
            ) in value.items()
        }

    if isinstance(
        value,
        (
            list,
            tuple,
        ),
    ):

        return [
            make_json_safe(
                nested_value
            )

            for nested_value in value
        ]

    return value


# ============================================================
# Stage loaders
# ============================================================

def load_event_stage(
    client,
    rows,
):

    job_config = (
        bigquery.LoadJobConfig(
            schema=EVENT_SCHEMA,

            write_disposition=(
                bigquery
                .WriteDisposition
                .WRITE_TRUNCATE
            ),
        )
    )


    json_safe_rows = [
        make_json_safe(
            row
        )

        for row in rows
    ]


    job = (
        client.load_table_from_json(
            json_safe_rows,
            EVENT_STAGE_TABLE,
            job_config=job_config,
        )
    )

    job.result()


def load_file_stage(
    client,
    rows,
):

    job_config = (
        bigquery.LoadJobConfig(
            schema=FILE_SCHEMA,

            write_disposition=(
                bigquery
                .WriteDisposition
                .WRITE_TRUNCATE
            ),
        )
    )


    json_safe_rows = [
        make_json_safe(
            row
        )

        for row in rows
    ]


    job = (
        client.load_table_from_json(
            json_safe_rows,
            FILE_STAGE_TABLE,
            job_config=job_config,
        )
    )

    job.result()


# ============================================================
# Atomic BigQuery merge
# ============================================================

def merge_stages(
    client,
):

    query = f"""
    BEGIN TRANSACTION;


    -- ========================================================
    -- IMPORTANT:
    --
    -- GTFS-Realtime may repeat the same vehicle observation
    -- across many consecutive snapshots.
    --
    -- event_key identifies the logical observation.
    --
    -- Deduplicate the staging table BEFORE the MERGE so that
    -- one logical event can never produce multiple inserts
    -- during the same BigQuery MERGE statement.
    --
    -- Earliest ingestion_timestamp wins, matching the
    -- canonical behavior of the PostgreSQL pipeline.
    -- ========================================================

    MERGE
        `{TARGET_TABLE}`
            AS target

    USING (

        SELECT
            * EXCEPT (
                event_rank
            )

        FROM (

            SELECT
                stage.*,

                ROW_NUMBER() OVER (

                    PARTITION BY
                        event_key

                    ORDER BY
                        ingestion_timestamp ASC,
                        feed_timestamp ASC,
                        entity_id ASC

                ) AS event_rank

            FROM
                `{EVENT_STAGE_TABLE}`
                    AS stage

        )

        WHERE
            event_rank = 1

    ) AS source


      ON target.event_key
            = source.event_key


    WHEN NOT MATCHED THEN

      INSERT (
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
        source.event_key,
        source.entity_id,
        source.vehicle_id,
        source.trip_id,
        source.route_id,
        source.schedule_relationship,
        source.direction_id,
        source.latitude,
        source.longitude,
        source.stop_id,
        source.current_stop_sequence,
        source.current_status,
        source.vehicle_timestamp,
        source.feed_timestamp,
        source.ingestion_timestamp
      );


    MERGE
        `{PROCESSED_TABLE}`
            AS target

    USING
        `{FILE_STAGE_TABLE}`
            AS source

      ON target.object_name
            = source.object_name


    WHEN NOT MATCHED THEN

      INSERT (
        object_name,
        ingestion_timestamp,
        processed_at,
        event_count
      )

      VALUES (
        source.object_name,
        source.ingestion_timestamp,
        CURRENT_TIMESTAMP(),
        source.event_count
      );


    COMMIT TRANSACTION;
    """


    client.query(
        query
    ).result()


# ============================================================
# Main
# ============================================================

def run_loader(
    lookback_minutes,
    max_files,
    full_scan=False,
):

    storage_client = (
        storage.Client(
            project=PROJECT_ID
        )
    )

    bigquery_client = (
        bigquery.Client(
            project=PROJECT_ID
        )
    )


    create_processed_table(
        bigquery_client
    )


    print(
        "======================================"
    )

    print(
        "GCS VEHICLE EVENTS -> BIGQUERY"
    )

    print(
        "======================================"
    )


    candidates = (
        discover_candidate_blobs(
            storage_client,
            bigquery_client,
            lookback_minutes,
            max_files,
            full_scan=full_scan,
        )
    )


    print(
        "Candidate files:",
        len(candidates),
    )


    if not candidates:

        print(
            "No new files to process."
        )

        return


    all_event_rows = []

    file_rows = []


    total_raw_events = 0


    for (
        ingestion_timestamp,
        blob,
    ) in candidates:

        print(
            "Reading:",
            blob.name,
        )


        data = (
            blob.download_as_bytes()
        )


        event_rows = (
            parse_snapshot(
                data,
                ingestion_timestamp,
            )
        )


        total_raw_events += (
            len(
                event_rows
            )
        )


        all_event_rows.extend(
            event_rows
        )


        file_rows.append(
            {
                "object_name":
                    blob.name,

                "ingestion_timestamp":
                    ingestion_timestamp,

                "event_count":
                    len(
                        event_rows
                    ),
            }
        )


    # ========================================================
    # First layer of protection:
    #
    # Deduplicate in Python before staging.
    #
    # The SQL MERGE also independently deduplicates staging,
    # giving the loader two layers of event-key protection.
    # ========================================================

    events_by_key = {}


    for row in all_event_rows:

        event_key = (
            row[
                "event_key"
            ]
        )


        existing = (
            events_by_key.get(
                event_key
            )
        )


        if (
            existing is None

            or row[
                "ingestion_timestamp"
            ]
            < existing[
                "ingestion_timestamp"
            ]
        ):

            events_by_key[
                event_key
            ] = row


    unique_event_rows = list(
        events_by_key.values()
    )


    print()

    print(
        "Raw event rows:",
        f"{total_raw_events:,}",
    )

    print(
        "Unique staged event keys:",
        f"{len(unique_event_rows):,}",
    )

    print(
        "Repeated observations removed "
        "before staging:",
        f"{(
            total_raw_events
            - len(unique_event_rows)
        ):,}",
    )


    if unique_event_rows:

        load_event_stage(
            bigquery_client,
            unique_event_rows,
        )

    else:

        raise RuntimeError(
            "Candidate files contained "
            "no vehicle events."
        )


    load_file_stage(
        bigquery_client,
        file_rows,
    )


    print(
        "Merging into BigQuery..."
    )


    merge_stages(
        bigquery_client
    )


    print()

    print(
        "======================================"
    )

    print(
        "BIGQUERY VEHICLE LOAD COMPLETE"
    )

    print(
        "======================================"
    )


# ============================================================
# CLI
# ============================================================

def main():

    parser = (
        argparse.ArgumentParser()
    )


    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=(
            DEFAULT_LOOKBACK_MINUTES
        ),
    )


    parser.add_argument(
        "--max-files",
        type=int,
        default=(
            DEFAULT_MAX_FILES
        ),
    )


    parser.add_argument(
        "--full-scan",
        action="store_true",
    )


    args = (
        parser.parse_args()
    )


    run_loader(
        lookback_minutes=(
            args.lookback_minutes
        ),

        max_files=(
            args.max_files
        ),

        full_scan=(
            args.full_scan
        ),
    )


if __name__ == "__main__":

    main()