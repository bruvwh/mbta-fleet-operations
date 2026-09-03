import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


DEFAULT_INPUT_PATH = (
    "gs://mbta-fleet-operations-alu/"
    "raw/lamp_subway"
)

DEFAULT_OUTPUT_PATH = (
    "gs://mbta-fleet-operations-alu/"
    "processed/lamp_stop_events"
)

DEFAULT_OUTPUT_PARTITIONS = 32


# ============================================================
# Arguments
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Build canonical MBTA LAMP historical "
            "stop events using Apache Spark."
        )
    )

    parser.add_argument(
        "--input-path",
        default=DEFAULT_INPUT_PATH,
        help="GCS path containing raw LAMP Parquet files.",
    )

    parser.add_argument(
        "--output-path",
        default=DEFAULT_OUTPUT_PATH,
        help="GCS destination for canonical stop events.",
    )

    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional inclusive start date: YYYY-MM-DD.",
    )

    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional inclusive end date: YYYY-MM-DD.",
    )

    parser.add_argument(
        "--output-partitions",
        type=int,
        default=DEFAULT_OUTPUT_PARTITIONS,
        help=(
            "Number of Spark partitions used before "
            "writing the output."
        ),
    )

    return parser.parse_args()


# ============================================================
# Argument validation
# ============================================================

def validate_date(value, argument_name):

    if value is None:
        return None

    try:
        return datetime.strptime(
            value,
            "%Y-%m-%d",
        ).date()

    except ValueError as error:

        raise ValueError(
            f"{argument_name} must use YYYY-MM-DD. "
            f"Received: {value}"
        ) from error


# ============================================================
# Main
# ============================================================

def main():

    args = parse_args()

    start_date = validate_date(
        args.start_date,
        "--start-date",
    )

    end_date = validate_date(
        args.end_date,
        "--end-date",
    )

    if (
        start_date is not None
        and end_date is not None
        and start_date > end_date
    ):
        raise ValueError(
            "--start-date cannot be after --end-date."
        )

    if args.output_partitions < 1:
        raise ValueError(
            "--output-partitions must be at least 1."
        )

    # ========================================================
    # Spark session
    # ========================================================

    spark = (
        SparkSession.builder
        .appName(
            "mbta-build-lamp-stop-events"
        )
        .getOrCreate()
    )

    # Epoch timestamps in LAMP represent absolute instants.
    # Keep Spark's session timezone deterministic.
    spark.conf.set(
        "spark.sql.session.timeZone",
        "UTC",
    )

    # When running a partial date backfill, overwrite only
    # service_date partitions produced by this job rather than
    # deleting unrelated historical partitions.
    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic",
    )

    print(
        "========================================"
    )
    print(
        "MBTA LAMP HISTORICAL STOP EVENT BUILD"
    )
    print(
        "========================================"
    )

    print(
        f"Input path:  {args.input_path}"
    )

    print(
        f"Output path: {args.output_path}"
    )

    print(
        f"Start date:  "
        f"{start_date if start_date else 'ALL'}"
    )

    print(
        f"End date:    "
        f"{end_date if end_date else 'ALL'}"
    )

    print(
        f"Output partitions: "
        f"{args.output_partitions}"
    )

    # ========================================================
    # 1. Read raw LAMP data
    # ========================================================

    print(
        "\nReading raw LAMP Parquet data..."
    )

    raw_df = (
        spark.read
        .option(
            "recursiveFileLookup",
            "true",
        )
        .parquet(
            args.input_path
        )
    )

    # ========================================================
    # 2. Standardize schema
    # ========================================================

    print(
        "Standardizing LAMP schema..."
    )

    standardized_df = (
        raw_df.select(
            F.to_date(
                F.col(
                    "service_date"
                ).cast("string"),
                "yyyyMMdd",
            ).alias(
                "service_date"
            ),

            F.col(
                "route_id"
            ).cast(
                "string"
            ).alias(
                "route_id"
            ),

            F.col(
                "branch_route_id"
            ).cast(
                "string"
            ).alias(
                "branch_route_id"
            ),

            F.col(
                "trunk_route_id"
            ).cast(
                "string"
            ).alias(
                "trunk_route_id"
            ),

            F.col(
                "trip_id"
            ).cast(
                "string"
            ).alias(
                "trip_id"
            ),

            F.col(
                "stop_id"
            ).cast(
                "string"
            ).alias(
                "stop_id"
            ),

            F.col(
                "parent_station"
            ).cast(
                "string"
            ).alias(
                "parent_station"
            ),

            F.col(
                "stop_sequence"
            ).cast(
                "integer"
            ).alias(
                "stop_sequence"
            ),

            F.col(
                "direction_id"
            ).cast(
                "integer"
            ).alias(
                "direction_id"
            ),

            F.col(
                "direction"
            ).cast(
                "string"
            ).alias(
                "direction"
            ),

            F.col(
                "direction_destination"
            ).cast(
                "string"
            ).alias(
                "direction_destination"
            ),

            F.col(
                "vehicle_id"
            ).cast(
                "string"
            ).alias(
                "vehicle_id"
            ),

            F.col(
                "vehicle_label"
            ).cast(
                "string"
            ).alias(
                "vehicle_label"
            ),

            F.col(
                "vehicle_consist"
            ).cast(
                "string"
            ).alias(
                "vehicle_consist"
            ),

            F.col(
                "stop_count"
            ).cast(
                "integer"
            ).alias(
                "stop_count"
            ),

            F.col(
                "start_time"
            ).cast(
                "integer"
            ).alias(
                "start_seconds"
            ),

            F.to_timestamp(
                F.from_unixtime(
                    F.col(
                        "move_timestamp"
                    )
                )
            ).alias(
                "move_timestamp"
            ),

            F.to_timestamp(
                F.from_unixtime(
                    F.col(
                        "stop_timestamp"
                    )
                )
            ).alias(
                "stop_timestamp"
            ),

            F.col(
                "travel_time_seconds"
            ).cast(
                "long"
            ).alias(
                "travel_time_seconds"
            ),

            F.col(
                "dwell_time_seconds"
            ).cast(
                "long"
            ).alias(
                "dwell_time_seconds"
            ),

            F.col(
                "headway_branch_seconds"
            ).cast(
                "long"
            ).alias(
                "headway_branch_seconds"
            ),

            F.col(
                "headway_trunk_seconds"
            ).cast(
                "long"
            ).alias(
                "headway_trunk_seconds"
            ),

            F.col(
                "scheduled_arrival_time"
            ).cast(
                "long"
            ).alias(
                "scheduled_arrival_seconds"
            ),

            F.col(
                "scheduled_departure_time"
            ).cast(
                "long"
            ).alias(
                "scheduled_departure_seconds"
            ),

            F.col(
                "scheduled_travel_time"
            ).cast(
                "long"
            ).alias(
                "scheduled_travel_time_seconds"
            ),

            F.col(
                "scheduled_headway_branch"
            ).cast(
                "long"
            ).alias(
                "scheduled_headway_branch_seconds"
            ),

            F.col(
                "scheduled_headway_trunk"
            ).cast(
                "long"
            ).alias(
                "scheduled_headway_trunk_seconds"
            ),
        )
    )

    # ========================================================
    # 3. Optional date filtering
    # ========================================================

    filtered_df = standardized_df

    if start_date is not None:

        filtered_df = (
            filtered_df.filter(
                F.col(
                    "service_date"
                )
                >= F.lit(
                    start_date
                )
            )
        )

    if end_date is not None:

        filtered_df = (
            filtered_df.filter(
                F.col(
                    "service_date"
                )
                <= F.lit(
                    end_date
                )
            )
        )

    # ========================================================
    # 4. Validate grain keys
    # ========================================================

    grain_columns = [
        "service_date",
        "trip_id",
        "stop_sequence",
        "stop_id",
    ]

    valid_df = (
        filtered_df.dropna(
            subset=grain_columns
        )
    )

    # ========================================================
    # 5. Deterministic duplicate handling
    # ========================================================

    hash_columns = [
        column
        for column in valid_df.columns
    ]

    row_hash = F.xxhash64(
        *[
            F.coalesce(
                F.col(
                    column
                ).cast(
                    "string"
                ),
                F.lit(
                    "<NULL>"
                ),
            )
            for column
            in hash_columns
        ]
    )

    ranked_df = (
        valid_df
        .withColumn(
            "_row_hash",
            row_hash,
        )
        .withColumn(
            "_row_number",
            F.row_number().over(
                Window.partitionBy(
                    *grain_columns
                ).orderBy(
                    F.col(
                        "stop_timestamp"
                    ).asc_nulls_last(),

                    F.col(
                        "move_timestamp"
                    ).asc_nulls_last(),

                    F.col(
                        "_row_hash"
                    ),
                )
            ),
        )
    )

    canonical_df = (
        ranked_df
        .filter(
            F.col(
                "_row_number"
            ) == 1
        )
        .drop(
            "_row_hash",
            "_row_number",
        )
    )

    # ========================================================
    # 6. Validation metrics
    # ========================================================

    print(
        "\nCalculating validation metrics..."
    )

    raw_count = (
        filtered_df.count()
    )

    valid_count = (
        valid_df.count()
    )

    canonical_count = (
        canonical_df.count()
    )

    invalid_key_count = (
        raw_count
        - valid_count
    )

    duplicate_count = (
        valid_count
        - canonical_count
    )

    service_date_count = (
        canonical_df.select(
            "service_date"
        )
        .distinct()
        .count()
    )

    print(
        f"Raw rows: "
        f"{raw_count:,}"
    )

    print(
        f"Invalid-key rows: "
        f"{invalid_key_count:,}"
    )

    print(
        f"Valid rows: "
        f"{valid_count:,}"
    )

    print(
        f"Duplicate grain rows removed: "
        f"{duplicate_count:,}"
    )

    print(
        f"Canonical output rows: "
        f"{canonical_count:,}"
    )

    print(
        f"Service dates: "
        f"{service_date_count:,}"
    )

    if canonical_count == 0:

        raise RuntimeError(
            "Canonical output contains zero rows. "
            "Nothing will be written."
        )

    # ========================================================
    # 7. Control output write parallelism
    # ========================================================

    print(
        f"\nPreparing "
        f"{args.output_partitions} "
        f"output partitions..."
    )

    output_df = (
        canonical_df.repartition(
            args.output_partitions,
            "service_date",
        )
    )

    # ========================================================
    # 8. Write canonical data
    # ========================================================

    print(
        "Writing canonical LAMP "
        "stop events to GCS..."
    )

    (
        output_df.write
        .mode(
            "overwrite"
        )
        .partitionBy(
            "service_date"
        )
        .parquet(
            args.output_path
        )
    )

    print(
        "\n========================================"
    )
    print(
        "LAMP STOP EVENT BUILD COMPLETE"
    )
    print(
        "========================================"
    )

    spark.stop()


if __name__ == "__main__":
    main()