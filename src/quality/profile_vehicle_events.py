import psycopg
from psycopg.rows import dict_row


def get_connection():
    return psycopg.connect(
        host="localhost",
        port=5432,
        dbname="mbta",
        user="mbta",
        password="mbta",
        row_factory=dict_row,
    )


def get_quality_summary(connection):

    query = """
    WITH ordered_events AS (
        SELECT
            vehicle_id,
            vehicle_timestamp,
            ingestion_timestamp,
            created_at,

            LAG(vehicle_timestamp) OVER (
                PARTITION BY vehicle_id
                ORDER BY ingestion_timestamp, created_at
            ) AS previous_vehicle_timestamp

        FROM vehicle_events

        WHERE vehicle_id IS NOT NULL
          AND vehicle_timestamp IS NOT NULL
    ),

    summary AS (
        SELECT
            COUNT(*) AS total_events,

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
                WHERE feed_timestamp < vehicle_timestamp
            ) AS negative_source_latency,

            AVG(
                EXTRACT(
                    EPOCH FROM
                    (feed_timestamp - vehicle_timestamp)
                )
            ) FILTER (
                WHERE feed_timestamp IS NOT NULL
                  AND vehicle_timestamp IS NOT NULL
            ) AS average_source_latency_seconds,

            MAX(
                EXTRACT(
                    EPOCH FROM
                    (feed_timestamp - vehicle_timestamp)
                )
            ) FILTER (
                WHERE feed_timestamp IS NOT NULL
                  AND vehicle_timestamp IS NOT NULL
            ) AS maximum_source_latency_seconds,

            PERCENTILE_CONT(0.5)
            WITHIN GROUP (
                ORDER BY EXTRACT(
                    EPOCH FROM
                    (feed_timestamp - vehicle_timestamp)
                )
            )
            FILTER (
                WHERE feed_timestamp IS NOT NULL
                  AND vehicle_timestamp IS NOT NULL
            ) AS median_source_latency_seconds,

            PERCENTILE_CONT(0.95)
            WITHIN GROUP (
                ORDER BY EXTRACT(
                    EPOCH FROM
                    (feed_timestamp - vehicle_timestamp)
                )
            )
            FILTER (
                WHERE feed_timestamp IS NOT NULL
                  AND vehicle_timestamp IS NOT NULL
            ) AS p95_source_latency_seconds

        FROM vehicle_events
    )

    SELECT
        summary.*,

        (
            SELECT COUNT(*)
            FROM ordered_events
            WHERE previous_vehicle_timestamp IS NOT NULL
              AND vehicle_timestamp
                  < previous_vehicle_timestamp
        ) AS out_of_order_events

    FROM summary;
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchone()


def get_highest_latency_events(connection):

    query = """
    SELECT
        vehicle_id,
        trip_id,
        route_id,
        vehicle_timestamp,
        feed_timestamp,

        EXTRACT(
            EPOCH FROM
            (feed_timestamp - vehicle_timestamp)
        ) AS source_latency_seconds

    FROM vehicle_events

    WHERE vehicle_timestamp IS NOT NULL
      AND feed_timestamp IS NOT NULL

    ORDER BY source_latency_seconds DESC

    LIMIT 5;
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()


def print_report(summary, highest_latency_events):

    print("=== VEHICLE EVENT DATA QUALITY REPORT ===")
    print()

    print(f"Total events: {summary['total_events']}")
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
        f"{summary['average_source_latency_seconds']} seconds"
    )

    print(
        f"Median source latency: "
        f"{summary['median_source_latency_seconds']} seconds"
    )

    print(
        f"95th percentile source latency: "
        f"{summary['p95_source_latency_seconds']} seconds"
    )

    print(
        f"Maximum source latency: "
        f"{summary['maximum_source_latency_seconds']} seconds"
    )

    print()
    print("=== TOP 5 HIGHEST LATENCY EVENTS ===")

    for event in highest_latency_events:
        print(event)


if __name__ == "__main__":

    connection = get_connection()

    try:
        summary = get_quality_summary(connection)

        highest_latency_events = (
            get_highest_latency_events(connection)
        )

        print_report(
            summary,
            highest_latency_events,
        )

    finally:
        connection.close()