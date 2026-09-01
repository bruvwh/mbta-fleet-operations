import psycopg


def get_connection():
    return psycopg.connect(
        host="localhost",
        port=5432,
        dbname="mbta",
        user="mbta",
        password="mbta",
    )


def print_section(title):
    print()
    print("=" * 60)
    print(title)
    print("=" * 60)


def profile_overview(cursor):
    print_section("HISTORICAL LAMP OVERVIEW")

    cursor.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT service_date) AS service_days,
            MIN(service_date) AS first_service_date,
            MAX(service_date) AS last_service_date,
            COUNT(DISTINCT trip_id) AS distinct_trip_ids
        FROM historical_stop_events;
    """)

    row = cursor.fetchone()

    print(f"Total stop events:     {row[0]:,}")
    print(f"Service days:          {row[1]:,}")
    print(f"First service date:    {row[2]}")
    print(f"Last service date:     {row[3]}")
    print(f"Distinct trip IDs:     {row[4]:,}")


def profile_missing_values(cursor):
    print_section("MISSING VALUES")

    cursor.execute("""
        SELECT
            COUNT(*) FILTER (
                WHERE route_id IS NULL
            ) AS missing_route_id,

            COUNT(*) FILTER (
                WHERE vehicle_id IS NULL
            ) AS missing_vehicle_id,

            COUNT(*) FILTER (
                WHERE move_timestamp IS NULL
                  AND stop_timestamp IS NULL
            ) AS missing_observed_timestamp,

            COUNT(*) FILTER (
                WHERE scheduled_arrival_seconds IS NULL
                  AND scheduled_departure_seconds IS NULL
            ) AS missing_scheduled_timestamp

        FROM historical_stop_events;
    """)

    row = cursor.fetchone()

    print(f"Missing route_id:             {row[0]:,}")
    print(f"Missing vehicle_id:           {row[1]:,}")
    print(f"Missing observed timestamps:  {row[2]:,}")
    print(f"Missing scheduled timestamps: {row[3]:,}")


def profile_duplicates(cursor):
    print_section("DUPLICATE CHECK")

    cursor.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT
                service_date,
                trip_id,
                stop_sequence,
                stop_id
            FROM historical_stop_events
            GROUP BY
                service_date,
                trip_id,
                stop_sequence,
                stop_id
            HAVING COUNT(*) > 1
        ) duplicates;
    """)

    duplicate_keys = cursor.fetchone()[0]

    print(f"Duplicate canonical keys: {duplicate_keys:,}")


def profile_registry_consistency(cursor):
    print_section("REGISTRY CONSISTENCY")

    cursor.execute("""
        SELECT COUNT(*)
        FROM historical_stop_events events
        JOIN historical_lamp_files files
          ON events.file_checksum = files.file_checksum
        WHERE events.service_date <> files.service_date;
    """)

    mismatched_dates = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*)
        FROM historical_lamp_files
        WHERE loaded_at IS NULL;
    """)

    unloaded_files = cursor.fetchone()[0]

    print(f"Rows with service-date mismatch: {mismatched_dates:,}")
    print(f"Registered but unloaded files:   {unloaded_files:,}")


def profile_metric_ranges(cursor):
    print_section("METRIC RANGES")

    cursor.execute("""
        SELECT
            MIN(travel_time_seconds),
            AVG(travel_time_seconds),
            MAX(travel_time_seconds),

            MIN(dwell_time_seconds),
            AVG(dwell_time_seconds),
            MAX(dwell_time_seconds),

            MIN(headway_branch_seconds),
            MAX(headway_branch_seconds),

            MIN(headway_trunk_seconds),
            MAX(headway_trunk_seconds)

        FROM historical_stop_events;
    """)

    row = cursor.fetchone()

    print(
        "Travel time seconds: "
        f"min={row[0]}, "
        f"avg={round(row[1], 2) if row[1] is not None else None}, "
        f"max={row[2]}"
    )

    print(
        "Dwell time seconds:  "
        f"min={row[3]}, "
        f"avg={round(row[4], 2) if row[4] is not None else None}, "
        f"max={row[5]}"
    )

    print(
        "Branch headway:      "
        f"min={row[6]}, max={row[7]}"
    )

    print(
        "Trunk headway:       "
        f"min={row[8]}, max={row[9]}"
    )


def profile_negative_metrics(cursor):
    print_section("NEGATIVE VALUE DIAGNOSTICS")

    cursor.execute("""
        SELECT
            COUNT(*) FILTER (
                WHERE travel_time_seconds < 0
            ),

            COUNT(*) FILTER (
                WHERE dwell_time_seconds < 0
            ),

            COUNT(*) FILTER (
                WHERE headway_branch_seconds < 0
            ),

            COUNT(*) FILTER (
                WHERE headway_trunk_seconds < 0
            )

        FROM historical_stop_events;
    """)

    row = cursor.fetchone()

    print(f"Negative travel times:   {row[0]:,}")
    print(f"Negative dwell times:    {row[1]:,}")
    print(f"Negative branch headway: {row[2]:,}")
    print(f"Negative trunk headway:  {row[3]:,}")

    print()
    print(
        "These are diagnostics only. "
        "We are not automatically treating negative values "
        "as invalid until we confirm the source semantics."
    )


def profile_rows_by_date(cursor):
    print_section("ROWS BY SERVICE DATE")

    cursor.execute("""
        SELECT
            service_date,
            COUNT(*) AS row_count,
            COUNT(DISTINCT trip_id) AS trip_count
        FROM historical_stop_events
        GROUP BY service_date
        ORDER BY service_date;
    """)

    rows = cursor.fetchall()

    for service_date, row_count, trip_count in rows:
        print(
            f"{service_date} | "
            f"rows={row_count:,} | "
            f"trips={trip_count:,}"
        )

def profile_metric_percentiles(cursor):
    print_section("METRIC DISTRIBUTIONS")

    cursor.execute("""
        SELECT
            percentile_cont(0.50)
                WITHIN GROUP (ORDER BY travel_time_seconds),
            percentile_cont(0.95)
                WITHIN GROUP (ORDER BY travel_time_seconds),
            percentile_cont(0.99)
                WITHIN GROUP (ORDER BY travel_time_seconds),

            percentile_cont(0.50)
                WITHIN GROUP (ORDER BY dwell_time_seconds),
            percentile_cont(0.95)
                WITHIN GROUP (ORDER BY dwell_time_seconds),
            percentile_cont(0.99)
                WITHIN GROUP (ORDER BY dwell_time_seconds)

        FROM historical_stop_events;
    """)

    row = cursor.fetchone()

    print(
        f"Travel time: "
        f"p50={row[0]:.0f}s | "
        f"p95={row[1]:.0f}s | "
        f"p99={row[2]:.0f}s"
    )

    print(
        f"Dwell time:  "
        f"p50={row[3]:.0f}s | "
        f"p95={row[4]:.0f}s | "
        f"p99={row[5]:.0f}s"
    )


def main():
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            profile_overview(cursor)
            profile_missing_values(cursor)
            profile_duplicates(cursor)
            profile_registry_consistency(cursor)
            profile_metric_ranges(cursor)
            profile_metric_percentiles(cursor)
            profile_negative_metrics(cursor)
            profile_rows_by_date(cursor)

    finally:
        connection.close()

    print()
    print("=" * 60)
    print("HISTORICAL LAMP PROFILE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()