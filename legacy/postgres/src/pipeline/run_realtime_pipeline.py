from pathlib import Path
import subprocess
import sys
import time

import psycopg


SQL_DIRECTORY = Path("sql")

PIPELINE_NAME = "realtime_pipeline"

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


# ============================================================
# Pipeline run logging
# ============================================================

def start_pipeline_run():
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO pipeline_runs (
                    pipeline_name,
                    status
                )

                VALUES (
                    %s,
                    'RUNNING'
                )

                RETURNING run_id;
                """,
                (
                    PIPELINE_NAME,
                ),
            )

            run_id = cursor.fetchone()[0]

        connection.commit()

        return run_id

    finally:
        connection.close()


def finish_pipeline_run(
    run_id,
    status,
    duration_seconds,
    events_inserted,
    error_message=None,
):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE pipeline_runs

                SET
                    finished_at = NOW(),
                    status = %s,
                    duration_seconds = %s,
                    events_inserted = %s,
                    error_message = %s

                WHERE run_id = %s;
                """,
                (
                    status,
                    duration_seconds,
                    events_inserted,
                    error_message,
                    run_id,
                ),
            )

        connection.commit()

    finally:
        connection.close()


# ============================================================
# Event-count tracking
#
# vehicle_event_registry contains exactly one row per globally
# unique realtime event, so its count lets us measure how many
# new events were added during this pipeline run.
# ============================================================

def get_registry_event_count():
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM vehicle_event_registry;
                """
            )

            return cursor.fetchone()[0]

    finally:
        connection.close()


# ============================================================
# Realtime loading
# ============================================================

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


# ============================================================
# SQL transformations
# ============================================================

def run_sql_file(connection, filename):
    filepath = SQL_DIRECTORY / filename

    print(f"\n=== RUNNING {filename} ===")

    sql = filepath.read_text()

    start_time = time.perf_counter()

    with connection.cursor() as cursor:
        cursor.execute(sql)

    connection.commit()

    elapsed_seconds = (
        time.perf_counter()
        - start_time
    )

    print(
        f"Completed: {filename} "
        f"({elapsed_seconds:.2f} seconds)"
    )


# ============================================================
# Data quality
# ============================================================

def run_quality_checks():
    print(
        "\n=== RUNNING DATA QUALITY CHECKS ==="
    )

    subprocess.run(
        [
            sys.executable,
            "src/quality/profile_vehicle_events.py",
        ],
        check=True,
    )

    print(
        "Data quality checks complete"
    )


# ============================================================
# Main realtime pipeline
# ============================================================

def run_realtime_pipeline():
    pipeline_start = time.perf_counter()

    run_id = start_pipeline_run()

    starting_event_count = (
        get_registry_event_count()
    )

    events_inserted = 0

    print(
        "\n================================"
    )
    print(
        "MBTA REALTIME PIPELINE"
    )
    print(
        "================================"
    )

    print(
        f"Pipeline run ID: {run_id}"
    )

    try:

        # ====================================================
        # Step 1:
        # Load new raw .pb snapshots into PostgreSQL
        # ====================================================

        run_realtime_loader()


        # Measure how many globally unique events were added.

        ending_event_count = (
            get_registry_event_count()
        )

        events_inserted = (
            ending_event_count
            - starting_event_count
        )


        # ====================================================
        # Step 2:
        # Run realtime transformations
        # ====================================================

        connection = get_connection()

        try:

            for filename in (
                REALTIME_TRANSFORMATIONS
            ):

                run_sql_file(
                    connection,
                    filename,
                )

        except Exception:

            connection.rollback()

            print(
                "\nREALTIME TRANSFORMATION FAILED"
            )

            raise

        finally:

            connection.close()


        # ====================================================
        # Step 3:
        # Run data-quality profiling
        # ====================================================

        run_quality_checks()


        # ====================================================
        # Successful completion
        # ====================================================

        pipeline_seconds = (
            time.perf_counter()
            - pipeline_start
        )

        finish_pipeline_run(
            run_id=run_id,
            status="SUCCESS",
            duration_seconds=(
                pipeline_seconds
            ),
            events_inserted=(
                events_inserted
            ),
        )


        print(
            f"\nEvents inserted this run: "
            f"{events_inserted}"
        )

        print(
            f"Total realtime pipeline time: "
            f"{pipeline_seconds:.2f} seconds"
        )

        print(
            "\n================================"
        )
        print(
            "REALTIME PIPELINE COMPLETE"
        )
        print(
            "================================"
        )


    except Exception as error:

        pipeline_seconds = (
            time.perf_counter()
            - pipeline_start
        )


        # Some loader batches may already have committed before
        # a later failure. Recount the registry so the failed
        # run still records how many events were successfully
        # inserted before the error.

        try:

            current_event_count = (
                get_registry_event_count()
            )

            events_inserted = max(
                0,
                current_event_count
                - starting_event_count,
            )

        except Exception:

            # Preserve the original pipeline error even if the
            # database cannot be queried during failure handling.

            pass


        try:

            finish_pipeline_run(
                run_id=run_id,
                status="FAILED",
                duration_seconds=(
                    pipeline_seconds
                ),
                events_inserted=(
                    events_inserted
                ),
                error_message=str(error),
            )

        except Exception as logging_error:

            print(
                "\nWARNING: Failed to update "
                "pipeline run log:"
            )

            print(
                logging_error
            )


        print(
            "\n================================"
        )
        print(
            "REALTIME PIPELINE FAILED"
        )
        print(
            "================================"
        )

        print(
            f"Run ID: {run_id}"
        )

        print(
            f"Error: {error}"
        )

        raise


if __name__ == "__main__":
    run_realtime_pipeline()