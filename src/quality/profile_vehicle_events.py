import argparse
import time
from datetime import timedelta

import psycopg
from psycopg.rows import dict_row


DEFAULT_RECENT_MINUTES = 30


def get_connection():

    return psycopg.connect(
        host="localhost",
        port=5432,
        dbname="mbta",
        user="mbta",
        password="mbta",
        row_factory=dict_row,
    )


# ============================================================
# Quality window
# ============================================================

def get_latest_ingestion_timestamp(connection):

    with connection.cursor() as cursor:

        cursor.execute(
            """
            SELECT MAX(ingestion_timestamp)
            AS latest_ingestion_timestamp
            FROM vehicle_events;
            """
        )

        row = cursor.fetchone()

        return row[
            "latest_ingestion_timestamp"
        ]


def get_quality_window(
    connection,
    full_history,
    recent_minutes,
):

    if full_history:

        return None, None


    latest_timestamp = (
        get_latest_ingestion_timestamp(
            connection
        )
    )


    if latest_timestamp is None:

        return None, None


    start_timestamp = (
        latest_timestamp
        - timedelta(
            minutes=recent_minutes
        )
    )


    return (
        start_timestamp,
        latest_timestamp,
    )


# ============================================================
# Quality summary
# ============================================================

def get_quality_summary(
    connection,
    start_timestamp=None,
    end_timestamp=None,
):

    if start_timestamp is None:

        time_filter = ""

        parameters = ()

    else:

        time_filter = """
            WHERE ingestion_timestamp >= %s
              AND ingestion_timestamp <= %s
        """

        parameters = (
            start_timestamp,
            end_timestamp,
        )


    query = f"""
    WITH quality_events AS (

        SELECT
            vehicle_id,
            trip_id,
            route_id,

            latitude,
            longitude,

            vehicle_timestamp,
            feed_timestamp,
            ingestion_timestamp,
            created_at

        FROM vehicle_events

        {time_filter}
    ),


    ordered_events AS (

        SELECT
            vehicle_id,
            vehicle_timestamp,
            ingestion_timestamp,
            created_at,

            LAG(vehicle_timestamp) OVER (

                PARTITION BY vehicle_id

                ORDER BY
                    ingestion_timestamp,
                    created_at

            ) AS previous_vehicle_timestamp

        FROM quality_events

        WHERE vehicle_id IS NOT NULL
          AND vehicle_timestamp IS NOT NULL
    ),


    summary AS (

        SELECT

            COUNT(*)
                AS total_events,


            COUNT(*) FILTER (

                WHERE vehicle_id IS NULL
                   OR vehicle_id = ''

            ) AS missing_vehicle_ids,


            COUNT(*) FILTER (

                WHERE trip_id IS NULL
                   OR trip_id = ''

            ) AS missing_trip_ids,


            COUNT(*) FILTER (

                WHERE route_id IS NULL
                   OR route_id = ''

            ) AS missing_route_ids,


            COUNT(*) FILTER (

                WHERE vehicle_timestamp IS NULL

            ) AS missing_vehicle_timestamps,


            COUNT(*) FILTER (

                WHERE latitude IS NULL
                   OR longitude IS NULL

            ) AS missing_coordinates,


            COUNT(*) FILTER (

                WHERE latitude NOT BETWEEN -90 AND 90

                   OR longitude NOT BETWEEN -180 AND 180

            ) AS invalid_coordinates,


            COUNT(*) FILTER (

                WHERE feed_timestamp
                    < vehicle_timestamp

            ) AS negative_source_latency,


            AVG(

                EXTRACT(
                    EPOCH FROM (
                        feed_timestamp
                        - vehicle_timestamp
                    )
                )

            ) FILTER (

                WHERE feed_timestamp IS NOT NULL

                  AND vehicle_timestamp
                      IS NOT NULL

            ) AS average_source_latency_seconds,


            MAX(

                EXTRACT(
                    EPOCH FROM (
                        feed_timestamp
                        - vehicle_timestamp
                    )
                )

            ) FILTER (

                WHERE feed_timestamp IS NOT NULL

                  AND vehicle_timestamp
                      IS NOT NULL

            ) AS maximum_source_latency_seconds,


            PERCENTILE_CONT(0.5)

            WITHIN GROUP (

                ORDER BY

                    EXTRACT(
                        EPOCH FROM (
                            feed_timestamp
                            - vehicle_timestamp
                        )
                    )

            )

            FILTER (

                WHERE feed_timestamp IS NOT NULL

                  AND vehicle_timestamp
                      IS NOT NULL

            ) AS median_source_latency_seconds,


            PERCENTILE_CONT(0.95)

            WITHIN GROUP (

                ORDER BY

                    EXTRACT(
                        EPOCH FROM (
                            feed_timestamp
                            - vehicle_timestamp
                        )
                    )

            )

            FILTER (

                WHERE feed_timestamp IS NOT NULL

                  AND vehicle_timestamp
                      IS NOT NULL

            ) AS p95_source_latency_seconds


        FROM quality_events
    )


    SELECT

        summary.*,


        (

            SELECT COUNT(*)

            FROM ordered_events

            WHERE
                previous_vehicle_timestamp
                    IS NOT NULL

                AND vehicle_timestamp
                    < previous_vehicle_timestamp

        ) AS out_of_order_events


    FROM summary;
    """


    with connection.cursor() as cursor:

        cursor.execute(
            query,
            parameters,
        )

        return cursor.fetchone()


# ============================================================
# Highest-latency events
# ============================================================

def get_highest_latency_events(
    connection,
    start_timestamp=None,
    end_timestamp=None,
):

    if start_timestamp is None:

        time_filter = ""

        parameters = ()

    else:

        time_filter = """
            AND ingestion_timestamp >= %s
            AND ingestion_timestamp <= %s
        """

        parameters = (
            start_timestamp,
            end_timestamp,
        )


    query = f"""
    SELECT

        vehicle_id,
        trip_id,
        route_id,

        vehicle_timestamp,
        feed_timestamp,

        EXTRACT(
            EPOCH FROM (
                feed_timestamp
                - vehicle_timestamp
            )
        ) AS source_latency_seconds


    FROM vehicle_events


    WHERE
        vehicle_timestamp IS NOT NULL

        AND feed_timestamp IS NOT NULL

        {time_filter}


    ORDER BY
        source_latency_seconds DESC


    LIMIT 5;
    """


    with connection.cursor() as cursor:

        cursor.execute(
            query,
            parameters,
        )

        return cursor.fetchall()


# ============================================================
# Report
# ============================================================

def print_report(
    summary,
    highest_latency_events,
    full_history,
    start_timestamp,
    end_timestamp,
):

    print(
        "=== VEHICLE EVENT DATA QUALITY REPORT ==="
    )

    print()


    if full_history:

        print(
            "Quality scope: FULL HISTORY"
        )

    else:

        print(
            "Quality scope: RECENT WINDOW"
        )

        print(
            f"Window start: {start_timestamp}"
        )

        print(
            f"Window end:   {end_timestamp}"
        )


    print()


    print(
        f"Events checked: "
        f"{summary['total_events']}"
    )

    print()


    print(
        f"Missing vehicle IDs: "
        f"{summary['missing_vehicle_ids']}"
    )


    print(
        f"Missing trip IDs: "
        f"{summary['missing_trip_ids']}"
    )


    print(
        f"Missing route IDs: "
        f"{summary['missing_route_ids']}"
    )


    print(
        f"Missing vehicle timestamps: "
        f"{summary['missing_vehicle_timestamps']}"
    )


    print(
        f"Missing coordinates: "
        f"{summary['missing_coordinates']}"
    )


    print(
        f"Invalid coordinates: "
        f"{summary['invalid_coordinates']}"
    )


    print(
        f"Negative source latency: "
        f"{summary['negative_source_latency']}"
    )


    print(
        f"Out-of-order events: "
        f"{summary['out_of_order_events']}"
    )


    print()


    print(
        f"Average source latency: "
        f"{summary['average_source_latency_seconds']} "
        f"seconds"
    )


    print(
        f"Median source latency: "
        f"{summary['median_source_latency_seconds']} "
        f"seconds"
    )


    print(
        f"95th percentile source latency: "
        f"{summary['p95_source_latency_seconds']} "
        f"seconds"
    )


    print(
        f"Maximum source latency: "
        f"{summary['maximum_source_latency_seconds']} "
        f"seconds"
    )


    print()

    print(
        "=== TOP 5 HIGHEST LATENCY EVENTS ==="
    )


    for event in highest_latency_events:

        print(event)


# ============================================================
# Main
# ============================================================

def run_quality_report(
    full_history=False,
    recent_minutes=DEFAULT_RECENT_MINUTES,
):

    start_time = time.perf_counter()


    connection = get_connection()


    try:

        (
            start_timestamp,
            end_timestamp,
        ) = get_quality_window(
            connection,
            full_history,
            recent_minutes,
        )


        summary = get_quality_summary(
            connection,
            start_timestamp,
            end_timestamp,
        )


        highest_latency_events = (
            get_highest_latency_events(
                connection,
                start_timestamp,
                end_timestamp,
            )
        )


        print_report(
            summary,
            highest_latency_events,
            full_history,
            start_timestamp,
            end_timestamp,
        )


    finally:

        connection.close()


    elapsed = (
        time.perf_counter()
        - start_time
    )


    print()

    print(
        f"Quality check runtime: "
        f"{elapsed:.2f} seconds"
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "Profile MBTA vehicle-event "
            "data quality."
        )
    )


    parser.add_argument(
        "--full",
        action="store_true",
        help=(
            "Audit the complete vehicle_events "
            "history instead of the recent window."
        ),
    )


    parser.add_argument(
        "--minutes",
        type=int,
        default=DEFAULT_RECENT_MINUTES,
        help=(
            "Recent ingestion window used for "
            "operational quality checks."
        ),
    )


    args = parser.parse_args()


    run_quality_report(
        full_history=args.full,
        recent_minutes=args.minutes,
    )