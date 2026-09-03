from pathlib import Path
import subprocess

import psycopg


SQL_DIRECTORY = Path("sql")


STATIC_TRANSFORMATIONS = [
    "build_gtfs_active_services.sql",
    "build_gtfs_trip_instances.sql",
    "build_gtfs_stop_instances.sql",
    "build_gtfs_trip_schedule_summary.sql",
]


def get_connection():
    return psycopg.connect(
        host="localhost",
        port=5432,
        dbname="mbta",
        user="mbta",
        password="mbta",
    )


def run_python_script(script):
    print(f"\n=== RUNNING {script} ===")

    subprocess.run(
        ["python", script],
        check=True,
    )

    print(f"Completed: {script}")


def run_sql_file(connection, filename):
    filepath = SQL_DIRECTORY / filename

    print(f"\n=== RUNNING {filename} ===")

    sql = filepath.read_text()

    with connection.cursor() as cursor:
        cursor.execute(sql)

    connection.commit()

    print(f"Completed: {filename}")


def get_unloaded_feed_count(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM gtfs_feeds
            WHERE schedule_loaded_at IS NULL
            """
        )

        return cursor.fetchone()[0]


def load_pending_gtfs_feeds():
    while True:

        with get_connection() as connection:
            unloaded_count = get_unloaded_feed_count(connection)

        if unloaded_count == 0:
            print("\nNo unloaded GTFS feeds remain.")
            break

        print(
            f"\nGTFS feeds waiting to load: "
            f"{unloaded_count}"
        )

        run_python_script(
            "src/loading/load_static_gtfs.py"
        )


def run_static_pipeline():

    print(
        "\n================================"
    )
    print(
        "MBTA STATIC GTFS PIPELINE"
    )
    print(
        "================================"
    )

    # Step 1:
    # Download current GTFS and register it
    # only if the checksum is new.
    run_python_script(
        "src/ingestion/static_gtfs.py"
    )

    # Step 2:
    # Load any GTFS feeds that exist in
    # gtfs_feeds but have not had their
    # schedule tables loaded yet.
    load_pending_gtfs_feeds()

    # Step 3:
    # Build derived static schedule tables.
    connection = get_connection()

    try:

        for filename in STATIC_TRANSFORMATIONS:

            run_sql_file(
                connection,
                filename,
            )

    except Exception:

        connection.rollback()

        print(
            "\nSTATIC PIPELINE FAILED"
        )

        raise

    finally:

        connection.close()

    print(
        "\n================================"
    )
    print(
        "STATIC PIPELINE COMPLETE"
    )
    print(
        "================================"
    )


if __name__ == "__main__":
    run_static_pipeline()