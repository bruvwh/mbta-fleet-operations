from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime


PROJECT_ROOT = (
    "/Users/andrewlu/Desktop/"
    "mbta-fleet-operations"
)

GCLOUD = "/opt/homebrew/bin/gcloud"
BQ = "/opt/homebrew/bin/bq"

PROJECT_ID = "mbta-fleet-operations-alu"
REGION = "us-central1"

SPARK_SCRIPT = (
    "gs://mbta-fleet-operations-alu/"
    "scripts/build_lamp_stop_events.py"
)

DBT_EXECUTABLE = (
    f"{PROJECT_ROOT}/.dbt-venv/bin/dbt"
)

DBT_PROJECT_DIRECTORY = (
    f"{PROJECT_ROOT}/dbt_mbta"
)


default_args = {
    "retries": 0,
    "retry_delay": timedelta(
        minutes=2
    ),
}


with DAG(
    dag_id="mbta_historical_cloud_pipeline",

    description=(
        "Process historical MBTA LAMP data "
        "with Dataproc Spark, BigQuery, and dbt."
    ),

    start_date=datetime(
        2026,
        9,
        1,
        tz="America/Los_Angeles",
    ),

    schedule=None,

    catchup=False,

    default_args=default_args,

    max_active_runs=1,

    tags=[
        "mbta",
        "historical",
        "spark",
        "bigquery",
        "dbt",
    ],

) as dag:

    # ========================================================
    # 1. Upload current Spark script
    # ========================================================

    upload_spark_script = BashOperator(
        task_id="upload_spark_script",

        bash_command=f"""
        set -e

        cd "{PROJECT_ROOT}"

        {GCLOUD} storage cp \
          src/spark/build_lamp_stop_events.py \
          {SPARK_SCRIPT}
        """,
    )


    # ========================================================
    # 2. Run managed Spark transformation
    #
    # dag_run.conf can optionally contain:
    #
    # {
    #   "start_date": "2026-08-01",
    #   "end_date": "2026-08-01",
    #   "output_partitions": 4,
    #   "region": "us-east1"
    # }
    #
    # Defaults:
    # start_date = 2026-07-01
    # end_date   = 2026-08-01
    # partitions = 32
    # region      = us-central1
    # ========================================================

    run_lamp_spark = BashOperator(
        task_id="run_lamp_spark",

        bash_command=f"""
        set -e

        START_DATE="{{{{ dag_run.conf.get(
            'start_date',
            '2026-07-01'
        ) }}}}"

        END_DATE="{{{{ dag_run.conf.get(
            'end_date',
            '2026-08-01'
        ) }}}}"

        OUTPUT_PARTITIONS="{{{{ dag_run.conf.get(
            'output_partitions',
            32
        ) }}}}"

        REGION="{{{{ dag_run.conf.get(
            'region',
            '{REGION}'
        ) }}}}"

        echo "Historical Spark backfill"
        echo "Start date: $START_DATE"
        echo "End date: $END_DATE"
        echo "Partitions: $OUTPUT_PARTITIONS"
        echo "Region: $REGION"

        {GCLOUD} dataproc batches submit pyspark \
          {SPARK_SCRIPT} \
          --project={PROJECT_ID} \
          --region="$REGION" \
          --version=3.0 \
          -- \
          --start-date="$START_DATE" \
          --end-date="$END_DATE" \
          --output-partitions="$OUTPUT_PARTITIONS"
        """,
    )


    # ========================================================
    # 3. Refresh native BigQuery historical table
    #
    # GCS-backed external table sees Spark output directly.
    # The native table does not, so rebuild it after Spark.
    # ========================================================

    refresh_bigquery_lamp_table = BashOperator(
        task_id="refresh_bigquery_lamp_table",

        bash_command=f"""
        set -e

        {BQ} query \
          --project_id={PROJECT_ID} \
          --location=US \
          --use_legacy_sql=false \
        '
        CREATE OR REPLACE TABLE
          `{PROJECT_ID}.mbta_analytics.lamp_stop_events`

        PARTITION BY
          service_date

        CLUSTER BY
          route_id,
          stop_id

        AS

        SELECT *
        FROM
          `{PROJECT_ID}.mbta_analytics.lamp_stop_events_external`
        ;
        '
        """,
    )


    # ========================================================
    # 4. Validate native BigQuery table
    # ========================================================

    validate_bigquery_lamp = BashOperator(
        task_id="validate_bigquery_lamp",

        bash_command=f"""
        set -e

        {BQ} query \
          --project_id={PROJECT_ID} \
          --location=US \
          --use_legacy_sql=false \
        '
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT service_date)
                AS service_date_count,
            MIN(service_date)
                AS first_service_date,
            MAX(service_date)
                AS last_service_date
        FROM
            `{PROJECT_ID}.mbta_analytics.lamp_stop_events`
        ;
        '
        """,
    )


    # ========================================================
    # 5. Build historical dbt branch
    # ========================================================

    build_historical_dbt = BashOperator(
        task_id="build_historical_dbt",

        bash_command=f"""
        set -e

        cd "{DBT_PROJECT_DIRECTORY}"

        "{DBT_EXECUTABLE}" build \
          --select +mart_historical_stop_baselines
        """,
    )


    # ========================================================
    # Dependencies
    # ========================================================

    (
        upload_spark_script
        >> run_lamp_spark
        >> refresh_bigquery_lamp_table
        >> validate_bigquery_lamp
        >> build_historical_dbt
    )