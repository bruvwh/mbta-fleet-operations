from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime


# ============================================================
# Project configuration
# ============================================================

PROJECT_ROOT = (
    "/Users/andrewlu/Desktop/"
    "mbta-fleet-operations"
)

PYTHON_EXECUTABLE = (
    f"{PROJECT_ROOT}/.venv/bin/python"
)

DBT_EXECUTABLE = (
    f"{PROJECT_ROOT}/.dbt-venv/bin/dbt"
)

DBT_PROJECT_DIRECTORY = (
    f"{PROJECT_ROOT}/dbt_mbta"
)

BQ = "/opt/homebrew/bin/bq"

PROJECT_ID = (
    "mbta-fleet-operations-alu"
)

DATASET_ID = (
    "mbta_analytics"
)


# ============================================================
# Default Airflow behavior
# ============================================================

default_args = {

    "retries": 1,

    "retry_delay": timedelta(
        minutes=1
    ),
}


# ============================================================
# DAG
# ============================================================

with DAG(

    dag_id=(
        "mbta_realtime_cloud_pipeline"
    ),

    description=(
        "Incremental MBTA realtime pipeline "
        "using GCS, BigQuery, and dbt."
    ),

    start_date=datetime(
        2026,
        9,
        1,
        tz="America/Los_Angeles",
    ),

    schedule="*/5 * * * *",

    catchup=False,

    max_active_runs=1,

    default_args=default_args,

    tags=[
        "mbta",
        "realtime",
        "gcs",
        "bigquery",
        "dbt",
    ],

) as dag:


    # ========================================================
    # 1. Parse new GCS protobuf snapshots into BigQuery
    #
    # The loader:
    #   - searches recent GCS ingestion partitions
    #   - skips already processed objects
    #   - parses GTFS-Realtime protobuf
    #   - generates deterministic event keys
    #   - merges new events into BigQuery
    # ========================================================

    load_vehicle_events_to_bigquery = BashOperator(

        task_id=(
            "load_vehicle_events_to_bigquery"
        ),

        bash_command=f"""
        set -e

        cd "{PROJECT_ROOT}"

        "{PYTHON_EXECUTABLE}" \
          src/loading/gcs_vehicle_events_to_bigquery.py \
          --lookback-minutes=30 \
          --max-files=500
        """,
    )


    # ========================================================
    # 2. Match new vehicle events to GTFS schedule
    #
    # This model is incremental, so it processes the recent
    # BigQuery vehicle-event window rather than rebuilding the
    # complete schedule-match history.
    # ========================================================

    build_cloud_schedule_matches = BashOperator(

        task_id=(
            "build_cloud_schedule_matches"
        ),

        bash_command=f"""
        set -e

        cd "{DBT_PROJECT_DIRECTORY}"

        "{DBT_EXECUTABLE}" build \
          --select \
          int_vehicle_event_schedule_matches_cloud
        """,
    )


    # ========================================================
    # 3. Build realtime analytics branch
    #
    # stg_vehicle_event_schedule_matches combines:
    #
    #   POSTGRES_LEGACY historical matches
    #             +
    #   BIGQUERY_CLOUD new matches
    #
    # Everything downstream therefore consumes one logical
    # schedule-match relation.
    # ========================================================

    build_realtime_analytics = BashOperator(

        task_id=(
            "build_realtime_analytics"
        ),

        bash_command=f"""
        set -e

        cd "{DBT_PROJECT_DIRECTORY}"

        "{DBT_EXECUTABLE}" build \
          --select \
          stg_vehicle_event_schedule_matches+
        """,
    )


    # ========================================================
    # 4. Print cloud pipeline freshness
    #
    # This does not alter data. It gives the Airflow log a
    # concise operational summary after each successful run.
    # ========================================================

    validate_realtime_freshness = BashOperator(

        task_id=(
            "validate_realtime_freshness"
        ),

        bash_command=f"""
        set -e

        {BQ} query \
          --project_id={PROJECT_ID} \
          --location=US \
          --use_legacy_sql=false \
        '
        SELECT
            COUNT(*) AS total_vehicle_events,

            MAX(
                ingestion_timestamp
            ) AS latest_vehicle_event,

            TIMESTAMP_DIFF(
                CURRENT_TIMESTAMP(),
                MAX(ingestion_timestamp),
                SECOND
            ) AS event_age_seconds

        FROM
            `{PROJECT_ID}.{DATASET_ID}.vehicle_events`
        ;
        '

        {BQ} query \
          --project_id={PROJECT_ID} \
          --location=US \
          --use_legacy_sql=false \
        '
        SELECT
            match_source,

            COUNT(*) AS match_count,

            MAX(
                event_ingestion_timestamp
            ) AS latest_event

        FROM
            `{PROJECT_ID}.{DATASET_ID}.stg_vehicle_event_schedule_matches`

        GROUP BY
            match_source

        ORDER BY
            match_source
        ;
        '
        """,
    )


    # ========================================================
    # Dependencies
    # ========================================================

    (
        load_vehicle_events_to_bigquery
        >> build_cloud_schedule_matches
        >> build_realtime_analytics
        >> validate_realtime_freshness
    )