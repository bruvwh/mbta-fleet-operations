import argparse
import subprocess
import sys

from datetime import datetime, timedelta
from pathlib import Path

import psycopg


SQL_DIRECTORY = Path("sql")


HISTORICAL_TRANSFORMATIONS = [
    "build_historical_stop_features.sql",
    "build_historical_baseline_eligible.sql",
    "build_historical_stop_baselines.sql",
    "build_historical_stop_baselines_fallback.sql",
]


def get_connection():
    return psycopg.connect(
        host="localhost",
        port=5432,
        dbname="mbta",
        user="mbta",
        password="mbta",
    )


def parse_date(value):
    return datetime.strptime(
        value,
        "%Y-%m-%d",
    ).date()


def generate_dates(start_date, end_date):
    current_date = start_date

    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def ingest_partition(service_date):
    date_string = service_date.strftime(
        "%Y-%m-%d"
    )

    print()
    print(
        f"=== INGESTING LAMP PARTITION "
        f"{date_string} ==="
    )

    subprocess.run(
        [
            sys.executable,
            "src/ingestion/historical_lamp.py",
            "--service-date",
            date_string,
        ],
        check=True,
    )


def load_historical_data():
    print()
    print(
        "=== LOADING HISTORICAL DATA ==="
    )

    subprocess.run(
        [
            sys.executable,
            "src/loading/load_historical_lamp.py",
        ],
        check=True,
    )

    print("Historical loading complete")


def run_quality_checks():
    print(
        "\n=== RUNNING HISTORICAL QUALITY CHECKS ==="
    )

    subprocess.run(
        [
            sys.executable,
            "src/quality/profile_historical_lamp.py",
        ],
        check=True,
    )

    print("Historical quality checks complete")


def run_sql_file(connection, filename):
    filepath = SQL_DIRECTORY / filename

    print(
        f"\n=== RUNNING {filename} ==="
    )

    sql = filepath.read_text()

    with connection.cursor() as cursor:
        cursor.execute(sql)

    connection.commit()

    print(
        f"Completed: {filename}"
    )


def run_historical_transformations():
    print(
        "\n=== RUNNING HISTORICAL TRANSFORMATIONS ==="
    )

    connection = get_connection()

    try:

        for filename in HISTORICAL_TRANSFORMATIONS:

            run_sql_file(
                connection,
                filename,
            )

    except Exception:

        connection.rollback()

        print(
            "\nHISTORICAL TRANSFORMATION FAILED"
        )

        raise

    finally:

        connection.close()

    print(
        "Historical transformations complete"
    )


def run_backfill(start_date, end_date):

    print(
        "\n================================"
    )

    print(
        "MBTA HISTORICAL LAMP BACKFILL"
    )

    print(
        "================================"
    )


    if end_date < start_date:
        raise ValueError(
            "end_date cannot be before start_date"
        )


    dates = list(
        generate_dates(
            start_date,
            end_date,
        )
    )

    print(
        f"Partitions requested: {len(dates)}"
    )


    # --------------------------------------------------------
    # Step 1:
    # Download raw LAMP partitions
    # --------------------------------------------------------

    for service_date in dates:

        ingest_partition(
            service_date
        )


    # --------------------------------------------------------
    # Step 2:
    # Load new raw files into historical_stop_events
    # --------------------------------------------------------

    load_historical_data()


    # --------------------------------------------------------
    # Step 3:
    # Validate loaded historical data
    # --------------------------------------------------------

    run_quality_checks()


    # --------------------------------------------------------
    # Step 4:
    # Rebuild historical feature and baseline layers
    # --------------------------------------------------------

    run_historical_transformations()


    print(
        "\n================================"
    )

    print(
        "HISTORICAL BACKFILL COMPLETE"
    )

    print(
        "================================"
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "Backfill MBTA LAMP historical "
            "subway performance data."
        )
    )


    parser.add_argument(
        "--start-date",
        required=True,
        help=(
            "Start date in YYYY-MM-DD format."
        ),
    )


    parser.add_argument(
        "--end-date",
        required=True,
        help=(
            "End date in YYYY-MM-DD format."
        ),
    )


    args = parser.parse_args()


    run_backfill(
        parse_date(
            args.start_date
        ),
        parse_date(
            args.end_date
        ),
    )