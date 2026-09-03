import csv
import hashlib
import io
import zipfile

from google.cloud import (
    bigquery,
    storage,
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

GTFS_PREFIX = (
    "raw/gtfs_static"
)

TARGET_TABLE = (
    f"{PROJECT_ID}."
    f"{DATASET_ID}."
    "gtfs_stops"
)

STAGE_TABLE = (
    f"{PROJECT_ID}."
    f"{DATASET_ID}."
    "_gtfs_stops_stage"
)


# ============================================================
# Schema
# ============================================================

STOP_SCHEMA = [

    bigquery.SchemaField(
        "feed_checksum",
        "STRING",
        mode="REQUIRED",
    ),

    bigquery.SchemaField(
        "stop_id",
        "STRING",
        mode="REQUIRED",
    ),

    bigquery.SchemaField(
        "stop_name",
        "STRING",
    ),

    bigquery.SchemaField(
        "stop_lat",
        "FLOAT",
    ),

    bigquery.SchemaField(
        "stop_lon",
        "FLOAT",
    ),

    bigquery.SchemaField(
        "location_type",
        "INTEGER",
    ),

    bigquery.SchemaField(
        "parent_station",
        "STRING",
    ),

    bigquery.SchemaField(
        "platform_code",
        "STRING",
    ),

    bigquery.SchemaField(
        "wheelchair_boarding",
        "INTEGER",
    ),

    bigquery.SchemaField(
        "source_object",
        "STRING",
        mode="REQUIRED",
    ),
]


# ============================================================
# Helpers
# ============================================================

def parse_int(value):

    if value is None:

        return None

    value = value.strip()

    if not value:

        return None

    return int(value)


def parse_float(value):

    if value is None:

        return None

    value = value.strip()

    if not value:

        return None

    return float(value)


def calculate_checksum(data):

    return hashlib.sha256(
        data
    ).hexdigest()


# ============================================================
# BigQuery target
# ============================================================

def create_target_table(
    client,
):

    query = f"""
    CREATE TABLE IF NOT EXISTS
        `{TARGET_TABLE}`
    (
        feed_checksum STRING NOT NULL,
        stop_id STRING NOT NULL,

        stop_name STRING,

        stop_lat FLOAT64,
        stop_lon FLOAT64,

        location_type INT64,

        parent_station STRING,
        platform_code STRING,

        wheelchair_boarding INT64,

        source_object STRING NOT NULL,

        loaded_at TIMESTAMP NOT NULL
    )

    CLUSTER BY
        feed_checksum,
        stop_id
    """

    client.query(
        query
    ).result()


# ============================================================
# GCS discovery
# ============================================================

def discover_gtfs_zips(
    storage_client,
):

    blobs = list(
        storage_client.list_blobs(
            BUCKET_NAME,
            prefix=GTFS_PREFIX,
        )
    )

    zip_blobs = [

        blob

        for blob in blobs

        if blob.name.lower().endswith(
            ".zip"
        )
    ]

    zip_blobs.sort(
        key=lambda blob:
            blob.name
    )

    return zip_blobs


# ============================================================
# Parse stops.txt
# ============================================================

def parse_gtfs_stops(
    zip_bytes,
    source_object,
):

    feed_checksum = (
        calculate_checksum(
            zip_bytes
        )
    )

    records = []

    with zipfile.ZipFile(
        io.BytesIO(
            zip_bytes
        )
    ) as gtfs_zip:

        if (
            "stops.txt"
            not in gtfs_zip.namelist()
        ):

            raise RuntimeError(
                f"stops.txt missing from "
                f"{source_object}"
            )

        with gtfs_zip.open(
            "stops.txt"
        ) as raw_file:

            text_file = (
                io.TextIOWrapper(
                    raw_file,
                    encoding="utf-8-sig",
                    newline="",
                )
            )

            reader = csv.DictReader(
                text_file
            )

            for row in reader:

                stop_id = (
                    row.get(
                        "stop_id"
                    )
                )

                if stop_id is None:

                    continue

                stop_id = (
                    stop_id.strip()
                )

                if not stop_id:

                    continue

                records.append(
                    {
                        "feed_checksum":
                            feed_checksum,

                        "stop_id":
                            stop_id,

                        "stop_name":
                            (
                                row.get(
                                    "stop_name"
                                )
                                or None
                            ),

                        "stop_lat":
                            parse_float(
                                row.get(
                                    "stop_lat"
                                )
                            ),

                        "stop_lon":
                            parse_float(
                                row.get(
                                    "stop_lon"
                                )
                            ),

                        "location_type":
                            parse_int(
                                row.get(
                                    "location_type"
                                )
                            ),

                        "parent_station":
                            (
                                row.get(
                                    "parent_station"
                                )
                                or None
                            ),

                        "platform_code":
                            (
                                row.get(
                                    "platform_code"
                                )
                                or None
                            ),

                        "wheelchair_boarding":
                            parse_int(
                                row.get(
                                    "wheelchair_boarding"
                                )
                            ),

                        "source_object":
                            source_object,
                    }
                )

    return records


# ============================================================
# Stage
# ============================================================

def load_stage(
    client,
    records,
):

    job_config = (
        bigquery.LoadJobConfig(
            schema=STOP_SCHEMA,

            write_disposition=(
                bigquery
                .WriteDisposition
                .WRITE_TRUNCATE
            ),
        )
    )

    job = (
        client.load_table_from_json(
            records,
            STAGE_TABLE,
            job_config=job_config,
        )
    )

    job.result()


# ============================================================
# Merge
# ============================================================

def merge_stops(
    client,
):

    query = f"""
    MERGE
        `{TARGET_TABLE}` AS target

    USING
        `{STAGE_TABLE}` AS source

      ON target.feed_checksum
            = source.feed_checksum

     AND target.stop_id
            = source.stop_id


    WHEN MATCHED THEN

      UPDATE SET

        stop_name =
            source.stop_name,

        stop_lat =
            source.stop_lat,

        stop_lon =
            source.stop_lon,

        location_type =
            source.location_type,

        parent_station =
            source.parent_station,

        platform_code =
            source.platform_code,

        wheelchair_boarding =
            source.wheelchair_boarding,

        source_object =
            source.source_object,

        loaded_at =
            CURRENT_TIMESTAMP()


    WHEN NOT MATCHED THEN

      INSERT (
        feed_checksum,
        stop_id,
        stop_name,
        stop_lat,
        stop_lon,
        location_type,
        parent_station,
        platform_code,
        wheelchair_boarding,
        source_object,
        loaded_at
      )

      VALUES (
        source.feed_checksum,
        source.stop_id,
        source.stop_name,
        source.stop_lat,
        source.stop_lon,
        source.location_type,
        source.parent_station,
        source.platform_code,
        source.wheelchair_boarding,
        source.source_object,
        CURRENT_TIMESTAMP()
      )
    """

    client.query(
        query
    ).result()


# ============================================================
# Main
# ============================================================

def main():

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

    print(
        "======================================"
    )

    print(
        "GTFS STOPS -> BIGQUERY"
    )

    print(
        "======================================"
    )

    create_target_table(
        bigquery_client
    )

    zip_blobs = (
        discover_gtfs_zips(
            storage_client
        )
    )

    print(
        "GTFS ZIP files found:",
        len(zip_blobs),
    )

    if not zip_blobs:

        raise RuntimeError(
            "No GTFS ZIP files found "
            f"under gs://{BUCKET_NAME}/"
            f"{GTFS_PREFIX}"
        )

    # --------------------------------------------------------
    # Deduplicate by logical BigQuery key in case identical
    # feed contents exist under multiple GCS object names.
    # --------------------------------------------------------

    records_by_key = {}

    for blob in zip_blobs:

        print(
            "Reading:",
            blob.name,
        )

        zip_bytes = (
            blob.download_as_bytes()
        )

        records = (
            parse_gtfs_stops(
                zip_bytes,
                blob.name,
            )
        )

        print(
            "Stops parsed:",
            f"{len(records):,}",
        )

        for record in records:

            key = (
                record[
                    "feed_checksum"
                ],
                record[
                    "stop_id"
                ],
            )

            records_by_key[
                key
            ] = record

    records = list(
        records_by_key.values()
    )

    print()
    print(
        "Unique feed-stop rows:",
        f"{len(records):,}",
    )

    if not records:

        raise RuntimeError(
            "No GTFS stop rows parsed."
        )

    print(
        "Loading staging table..."
    )

    load_stage(
        bigquery_client,
        records,
    )

    print(
        "Merging GTFS stops..."
    )

    merge_stops(
        bigquery_client
    )

    print()

    print(
        "======================================"
    )

    print(
        "GTFS STOP LOAD COMPLETE"
    )

    print(
        "======================================"
    )


if __name__ == "__main__":

    main()