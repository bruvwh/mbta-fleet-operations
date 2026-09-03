from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import (
    BashOperator,
)
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
)


# ============================================================
# Project configuration
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

PROJECT_PYTHON = (
    PROJECT_ROOT
    / ".venv"
    / "bin"
    / "python"
)

GCLOUD = (
    "/opt/homebrew/bin/gcloud"
)

GCS_BUCKET = (
    "gs://mbta-fleet-operations-alu"
)

POSTGRES_CONNECTION_ID = (
    "mbta_postgres"
)


# ============================================================
# DAG
#
# Runs every 5 minutes.
#
# max_active_runs=1 prevents multiple copies of this DAG from
# running concurrently.
# ============================================================

with DAG(
    dag_id="mbta_realtime_pipeline",

    description=(
        "Incremental MBTA realtime fleet "
        "operations pipeline"
    ),

    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz="America/New_York",
    ),

    schedule="*/5 * * * *",

    catchup=False,

    max_active_runs=1,

    default_args={
        "retries": 1,
        "retry_delay": timedelta(
            minutes=1
        ),
    },

    template_searchpath=[
        str(
            PROJECT_ROOT
            / "sql"
        )
    ],

    tags=[
        "mbta",
        "realtime",
        "data-engineering",
        "gcp",
    ],
) as dag:

    # ========================================================
    # 1. Synchronize raw VehiclePositions snapshots to GCS
    #
    # The collector continues writing raw .pb files locally.
    #
    # This task synchronizes any new files into the GCS raw
    # layer while preserving the ingestion_date partitions.
    #
    # gcloud storage rsync does not re-upload unchanged files.
    # ========================================================

    sync_vehicle_positions_to_gcs = (
        BashOperator(
            task_id=(
                "sync_vehicle_positions_to_gcs"
            ),

            bash_command=(
                f'"{GCLOUD}" storage rsync '
                "data/raw/vehicle_positions "
                f"{GCS_BUCKET}/raw/"
                "vehicle_positions "
                "--recursive"
            ),

            cwd=str(
                PROJECT_ROOT
            ),

            do_xcom_push=False,
        )
    )


    # ========================================================
    # 2. Incremental realtime loading
    #
    # The loader currently reads from the local raw layer.
    #
    # -u makes Python stdout unbuffered so progress appears
    # immediately in Airflow logs.
    # ========================================================

    load_realtime_data = BashOperator(
        task_id="load_realtime_data",

        bash_command=(
            f"{PROJECT_PYTHON} -u "
            "src/loading/"
            "incremental_vehicle_loader.py"
        ),

        cwd=str(
            PROJECT_ROOT
        ),

        do_xcom_push=False,
    )


    # ========================================================
    # 3. Collector coverage
    # ========================================================

    build_collector_coverage_windows = (
        SQLExecuteQueryOperator(
            task_id=(
                "build_collector_coverage_windows"
            ),

            conn_id=(
                POSTGRES_CONNECTION_ID
            ),

            sql=(
                "build_collector_coverage_windows.sql"
            ),

            handler=None,
        )
    )


    # ========================================================
    # 4. Match realtime observations to GTFS schedule
    # ========================================================

    build_vehicle_event_schedule_matches = (
        SQLExecuteQueryOperator(
            task_id=(
                "build_vehicle_event_schedule_matches"
            ),

            conn_id=(
                POSTGRES_CONNECTION_ID
            ),

            sql=(
                "build_vehicle_event_schedule_matches.sql"
            ),

            handler=None,
        )
    )


    # ========================================================
    # 5. Infer realtime stop events
    # ========================================================

    build_realtime_stop_events = (
        SQLExecuteQueryOperator(
            task_id=(
                "build_realtime_stop_events"
            ),

            conn_id=(
                POSTGRES_CONNECTION_ID
            ),

            sql=(
                "build_realtime_stop_events.sql"
            ),

            handler=None,
        )
    )


    # ========================================================
    # 6. Compare inferred stops with schedule
    # ========================================================

    build_realtime_stop_performance = (
        SQLExecuteQueryOperator(
            task_id=(
                "build_realtime_stop_performance"
            ),

            conn_id=(
                POSTGRES_CONNECTION_ID
            ),

            sql=(
                "build_realtime_stop_performance.sql"
            ),

            handler=None,
        )
    )


    # ========================================================
    # 7. Trip-level realtime summary
    # ========================================================

    build_trip_realtime_summary = (
        SQLExecuteQueryOperator(
            task_id=(
                "build_trip_realtime_summary"
            ),

            conn_id=(
                POSTGRES_CONNECTION_ID
            ),

            sql=(
                "build_trip_realtime_summary.sql"
            ),

            handler=None,
        )
    )


    # ========================================================
    # 8. Trip operations
    # ========================================================

    build_trip_operations = (
        SQLExecuteQueryOperator(
            task_id="build_trip_operations",

            conn_id=(
                POSTGRES_CONNECTION_ID
            ),

            sql=(
                "build_trip_operations.sql"
            ),

            handler=None,
        )
    )


    # ========================================================
    # 9. Update trip coverage
    # ========================================================

    update_trip_operations_coverage = (
        SQLExecuteQueryOperator(
            task_id=(
                "update_trip_operations_coverage"
            ),

            conn_id=(
                POSTGRES_CONNECTION_ID
            ),

            sql=(
                "update_trip_operations_coverage.sql"
            ),

            handler=None,
        )
    )


    # ========================================================
    # 10. Realtime stop features
    # ========================================================

    build_realtime_stop_features = (
        SQLExecuteQueryOperator(
            task_id=(
                "build_realtime_stop_features"
            ),

            conn_id=(
                POSTGRES_CONNECTION_ID
            ),

            sql=(
                "build_realtime_stop_features.sql"
            ),

            handler=None,
        )
    )


    # ========================================================
    # 11. Baseline eligibility
    # ========================================================

    build_realtime_baseline_eligible = (
        SQLExecuteQueryOperator(
            task_id=(
                "build_realtime_baseline_eligible"
            ),

            conn_id=(
                POSTGRES_CONNECTION_ID
            ),

            sql=(
                "build_realtime_baseline_eligible.sql"
            ),

            handler=None,
        )
    )


    # ========================================================
    # 12. Stop anomaly scoring
    # ========================================================

    build_realtime_stop_anomaly_scores = (
        SQLExecuteQueryOperator(
            task_id=(
                "build_realtime_stop_anomaly_scores"
            ),

            conn_id=(
                POSTGRES_CONNECTION_ID
            ),

            sql=(
                "build_realtime_stop_anomaly_scores.sql"
            ),

            handler=None,
        )
    )


    # ========================================================
    # 13. Trip anomaly summary
    # ========================================================

    build_trip_anomaly_summary = (
        SQLExecuteQueryOperator(
            task_id=(
                "build_trip_anomaly_summary"
            ),

            conn_id=(
                POSTGRES_CONNECTION_ID
            ),

            sql=(
                "build_trip_anomaly_summary.sql"
            ),

            handler=None,
        )
    )


    # ========================================================
    # 14. Trip anomaly classification
    # ========================================================

    build_trip_anomaly_classification = (
        SQLExecuteQueryOperator(
            task_id=(
                "build_trip_anomaly_classification"
            ),

            conn_id=(
                POSTGRES_CONNECTION_ID
            ),

            sql=(
                "build_trip_anomaly_classification.sql"
            ),

            handler=None,
        )
    )


    # ========================================================
    # 15. Route-hour operations
    # ========================================================

    build_route_hour_operations = (
        SQLExecuteQueryOperator(
            task_id=(
                "build_route_hour_operations"
            ),

            conn_id=(
                POSTGRES_CONNECTION_ID
            ),

            sql=(
                "build_route_hour_operations.sql"
            ),

            handler=None,
        )
    )


    # ========================================================
    # 16. Route-hour anomaly classification
    # ========================================================

    build_route_hour_anomaly_classification = (
        SQLExecuteQueryOperator(
            task_id=(
                "build_route_hour_anomaly_classification"
            ),

            conn_id=(
                POSTGRES_CONNECTION_ID
            ),

            sql=(
                "build_route_hour_anomaly_classification.sql"
            ),

            handler=None,
        )
    )


    # ========================================================
    # 17. Route-day operational output
    # ========================================================

    build_route_daily_operations = (
        SQLExecuteQueryOperator(
            task_id=(
                "build_route_daily_operations"
            ),

            conn_id=(
                POSTGRES_CONNECTION_ID
            ),

            sql=(
                "build_route_daily_operations.sql"
            ),

            handler=None,
        )
    )


    # ========================================================
    # 18. Data-quality profiling
    # ========================================================

    quality_checks = BashOperator(
        task_id="quality_checks",

        bash_command=(
            f"{PROJECT_PYTHON} -u "
            "src/quality/"
            "profile_vehicle_events.py"
        ),

        cwd=str(
            PROJECT_ROOT
        ),

        do_xcom_push=False,
    )


    # ========================================================
    # Dependencies
    # ========================================================

    (
        sync_vehicle_positions_to_gcs
        >> load_realtime_data
        >> build_collector_coverage_windows
        >> build_vehicle_event_schedule_matches
        >> build_realtime_stop_events
        >> build_realtime_stop_performance
        >> build_trip_realtime_summary
        >> build_trip_operations
        >> update_trip_operations_coverage
        >> build_realtime_stop_features
        >> build_realtime_baseline_eligible
        >> build_realtime_stop_anomaly_scores
        >> build_trip_anomaly_summary
        >> build_trip_anomaly_classification
        >> build_route_hour_operations
        >> build_route_hour_anomaly_classification
        >> build_route_daily_operations
        >> quality_checks
    )