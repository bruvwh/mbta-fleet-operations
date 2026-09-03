import csv
import hashlib
import io
import zipfile
from datetime import datetime
from pathlib import Path

import psycopg
import requests


GTFS_URL = "https://cdn.mbta.com/MBTA_GTFS.zip"
RAW_DIRECTORY = Path("data/raw/gtfs_static")


def download_gtfs():
    response = requests.get(GTFS_URL, timeout=60)
    response.raise_for_status()

    return response.content


def calculate_checksum(data):
    return hashlib.sha256(data).hexdigest()


def parse_gtfs_date(value):
    if not value:
        return None

    return datetime.strptime(
        value,
        "%Y%m%d"
    ).date()


def read_feed_info(data):
    with zipfile.ZipFile(io.BytesIO(data)) as gtfs_zip:

        with gtfs_zip.open("feed_info.txt") as file:

            text_file = io.TextIOWrapper(
                file,
                encoding="utf-8-sig"
            )

            reader = csv.DictReader(text_file)

            row = next(reader)

            return {
                "feed_version": row.get("feed_version"),
                "feed_start_date": parse_gtfs_date(
                    row.get("feed_start_date")
                ),
                "feed_end_date": parse_gtfs_date(
                    row.get("feed_end_date")
                ),
            }


def feed_already_exists(cursor, checksum):
    cursor.execute(
        """
        SELECT 1
        FROM gtfs_feeds
        WHERE feed_checksum = %s
        """,
        (checksum,),
    )

    return cursor.fetchone() is not None


def save_raw_feed(data, checksum):
    RAW_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True
    )

    filepath = (
        RAW_DIRECTORY
        / f"mbta_gtfs_{checksum[:12]}.zip"
    )

    filepath.write_bytes(data)

    return filepath


def register_feed(
    cursor,
    checksum,
    feed_info,
    filepath,
):
    cursor.execute(
        """
        INSERT INTO gtfs_feeds (
            feed_checksum,
            feed_version,
            feed_start_date,
            feed_end_date,
            file_path
        )
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            checksum,
            feed_info["feed_version"],
            feed_info["feed_start_date"],
            feed_info["feed_end_date"],
            str(filepath),
        ),
    )


def ingest_static_gtfs():
    print("Downloading current MBTA GTFS feed...")

    data = download_gtfs()

    checksum = calculate_checksum(data)

    connection = psycopg.connect(
        host="localhost",
        port=5432,
        dbname="mbta",
        user="mbta",
        password="mbta",
    )

    try:
        with connection.cursor() as cursor:

            if feed_already_exists(
                cursor,
                checksum,
            ):
                print("GTFS feed already ingested.")
                return

            feed_info = read_feed_info(data)

            filepath = save_raw_feed(
                data,
                checksum,
            )

            register_feed(
                cursor,
                checksum,
                feed_info,
                filepath,
            )

        connection.commit()

        print("New GTFS feed ingested.")
        print(
            f"Version: "
            f"{feed_info['feed_version']}"
        )
        print(
            f"Valid from: "
            f"{feed_info['feed_start_date']}"
        )
        print(
            f"Valid through: "
            f"{feed_info['feed_end_date']}"
        )
        print(f"Saved: {filepath}")

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    ingest_static_gtfs()