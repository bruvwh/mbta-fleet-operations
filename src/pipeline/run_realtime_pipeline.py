from pathlib import Path
import subprocess
import sys
import time

import psycopg


SQL_DIRECTORY = Path("sql")

REALTIME_TRANSFORMATIONS = [
    "build_collector_coverage_windows.sql",
    "build_vehicle_event_schedule_matches.sql",

    "build_realtime_stop_events.sql",
    "build_realtime_stop_performance.sql",

    "build_trip_realtime_summary.sql",
    "build_trip_operations.sql",
    "update_trip_operations_coverage.sql",

    # Operational feature layer
    "build_realtime_stop_features.sql",
    "build_realtime_baseline_eligible.sql",

    # Anomaly detection
    "build_realtime_stop_anomaly_scores.sql",
    "build_trip_anomaly_summary.sql",
    "build_trip_anomaly_classification.sql",

    # Stakeholder / analytics outputs
    "build_route_hour_operations.sql",
    "build_route_hour_anomaly_classification.sql",
    "build_route_daily_operations.sql",
]


def get_connection():
    return psycopg.connect(
        host="localhost",
        port=5432,
        dbname="mbta",
        user="mbta",
        password="mbta",
    )


def run_realtime_loader():
    print("\n=== LOADING REALTIME DATA ===")

    subprocess.run(
        [
            sys.executable,
            "src/loading/incremental_vehicle_loader.py",
        ],
        check=True,
    )

    print("Realtime loading complete")


def run_sql_file(connection, filename):
    filepath = SQL_DIRECTORY / filename

    print(f"\n=== RUNNING {filename} ===")

    sql = filepath.read_text()

    start_time = time.perf_counter()

    with connection.cursor() as cursor:
        cursor.execute(sql)

    connection.commit()

    elapsed_seconds = time.perf_counter() - start_time

    print(
        f"Completed: {filename} "
        f"({elapsed_seconds:.2f} seconds)"
    )


def run_quality_checks():
    print("\n=== RUNNING DATA QUALITY CHECKS ===")

    subprocess.run(
        [
            sys.executable,
            "src/quality/profile_vehicle_events.py",
        ],
        check=True,
    )

    print("Data quality checks complete")


def run_realtime_pipeline():
    pipeline_start = time.perf_counter()

    print("\n================================")
    print("MBTA REALTIME PIPELINE")
    print("================================")

    # Step 1: Load new raw .pb snapshots into PostgreSQL
    run_realtime_loader()

    # Step 2: Run realtime transformations
    connection = get_connection()

    try:
        for filename in REALTIME_TRANSFORMATIONS:
            run_sql_file(
                connection,
                filename,
            )

    except Exception:
        connection.rollback()
        print("\nREALTIME TRANSFORMATION FAILED")
        raise

    finally:
        connection.close()

    # Step 3: Run data-quality profiling
    run_quality_checks()

    pipeline_seconds = (
        time.perf_counter() - pipeline_start
    )

    print(
        f"\nTotal realtime pipeline time: "
        f"{pipeline_seconds:.2f} seconds"
    )    

    print("\n================================")
    print("REALTIME PIPELINE COMPLETE")
    print("================================")


if __name__ == "__main__":
    run_realtime_pipeline()