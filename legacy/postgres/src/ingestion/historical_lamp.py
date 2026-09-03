import hashlib
from datetime import datetime
from pathlib import Path

import psycopg
import requests


BASE_URL = (
    "https://performancedata.mbta.com/lamp/"
    "subway-on-time-performance-v1"
)

RAW_DIRECTORY = Path("data/raw/lamp_subway")


def get_connection():
    return psycopg.connect(
        host="localhost",
        port=5432,
        dbname="mbta",
        user="mbta",
        password="mbta",
    )


def calculate_checksum(data):
    return hashlib.sha256(data).hexdigest()


def build_url(service_date):
    date_string = service_date.strftime("%Y-%m-%d")

    return (
        f"{BASE_URL}/"
        f"{date_string}-subway-on-time-performance-v1.parquet"
    )


def download_file(service_date):
    url = build_url(service_date)

    print(f"Downloading: {url}")

    response = requests.get(
        url,
        timeout=60,
    )

    response.raise_for_status()

    return response.content


def save_raw_file(service_date, data):
    date_string = service_date.strftime("%Y-%m-%d")

    directory = (
        RAW_DIRECTORY
        / f"service_date={date_string}"
    )

    directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    filepath = (
        directory
        / f"{date_string}-subway-on-time-performance-v1.parquet"
    )

    filepath.write_bytes(data)

    return filepath


def file_already_registered(cursor, checksum):
    cursor.execute(
        """
        SELECT 1
        FROM historical_lamp_files
        WHERE file_checksum = %s
        """,
        (checksum,),
    )

    return cursor.fetchone() is not None


def ingest_date(service_date):
    data = download_file(service_date)

    checksum = calculate_checksum(data)

    connection = get_connection()

    try:
        with connection.cursor() as cursor:

            if file_already_registered(
                cursor,
                checksum,
            ):
                print(
                    f"Already ingested: "
                    f"{service_date}"
                )

                return

            filepath = save_raw_file(
                service_date,
                data,
            )

            cursor.execute(
                """
                INSERT INTO historical_lamp_files (
                    file_checksum,
                    service_date,
                    file_path
                )
                VALUES (%s, %s, %s)
                """,
                (
                    checksum,
                    service_date,
                    str(filepath),
                ),
            )

        connection.commit()

        print(f"Saved: {filepath}")

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Download one LAMP historical partition."
    )

    parser.add_argument(
        "--service-date",
        required=True,
        help="Service date in YYYY-MM-DD format.",
    )

    args = parser.parse_args()

    service_date = datetime.strptime(
        args.service_date,
        "%Y-%m-%d",
    ).date()

    ingest_date(service_date)