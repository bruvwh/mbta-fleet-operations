import psycopg


RECENT_HOURS = 2


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


def profile_status_counts(cursor):
    print_section("VEHICLE STATUS COUNTS")

    cursor.execute(
        """
        SELECT
            CASE current_status
                WHEN 0 THEN 'INCOMING_AT'
                WHEN 1 THEN 'STOPPED_AT'
                WHEN 2 THEN 'IN_TRANSIT_TO'
                ELSE 'UNKNOWN'
            END AS status,
            COUNT(*) AS observations

        FROM vehicle_events_v2

        WHERE
            ingestion_timestamp
                >= NOW() - (%s * INTERVAL '1 hour')

        GROUP BY current_status

        ORDER BY current_status;
        """,
        (RECENT_HOURS,),
    )

    for status, count in cursor.fetchall():
        print(f"{status:<15} {count:,}")


def profile_trip_stop_coverage(cursor):
    print_section("TRIP-STOP STATUS COVERAGE")

    cursor.execute(
        """
        WITH trip_stops AS (

            SELECT
                vehicle_id,
                trip_id,
                stop_id,
                current_stop_sequence,

                COUNT(*) AS observations,

                COUNT(*) FILTER (
                    WHERE current_status = 0
                ) AS incoming_count,

                COUNT(*) FILTER (
                    WHERE current_status = 1
                ) AS stopped_count,

                COUNT(*) FILTER (
                    WHERE current_status = 2
                ) AS transit_count

            FROM vehicle_events_v2

            WHERE
                ingestion_timestamp
                    >= NOW() - (%s * INTERVAL '1 hour')

                AND vehicle_id IS NOT NULL

                AND trip_id IS NOT NULL

                AND stop_id IS NOT NULL

                AND current_stop_sequence
                    IS NOT NULL

            GROUP BY
                vehicle_id,
                trip_id,
                stop_id,
                current_stop_sequence
        )

        SELECT
            COUNT(*) AS total_trip_stops,

            COUNT(*) FILTER (
                WHERE stopped_count > 0
            ) AS with_stopped,

            COUNT(*) FILTER (
                WHERE transit_count > 0
            ) AS with_transit,

            COUNT(*) FILTER (
                WHERE stopped_count > 0
                  AND transit_count > 0
            ) AS with_both,

            COUNT(*) FILTER (
                WHERE stopped_count = 0
                  AND transit_count > 0
            ) AS transit_only,

            COUNT(*) FILTER (
                WHERE stopped_count > 0
                  AND transit_count = 0
            ) AS stopped_only

        FROM trip_stops;
        """,
        (RECENT_HOURS,),
    )

    row = cursor.fetchone()

    total = row[0]

    print(f"Total trip-stop groups:     {total:,}")
    print(f"With STOPPED_AT:            {row[1]:,}")
    print(f"With IN_TRANSIT_TO:         {row[2]:,}")
    print(f"With both statuses:         {row[3]:,}")
    print(f"Transit only:               {row[4]:,}")
    print(f"Stopped only:               {row[5]:,}")

    if total:
        print()
        print(
            f"STOPPED_AT coverage: "
            f"{row[1] / total * 100:.2f}%"
        )


def profile_multiple_stopped_periods(cursor):
    print_section("MULTIPLE STOPPED PERIODS")

    cursor.execute(
        """
        WITH ordered AS (

            SELECT
                vehicle_id,
                trip_id,
                stop_id,
                current_stop_sequence,
                vehicle_timestamp,
                current_status,

                LAG(current_status) OVER (
                    PARTITION BY
                        vehicle_id,
                        trip_id,
                        stop_id,
                        current_stop_sequence

                    ORDER BY vehicle_timestamp
                ) AS previous_status

            FROM vehicle_events_v2

            WHERE
                ingestion_timestamp
                    >= NOW() - (%s * INTERVAL '1 hour')

                AND vehicle_id IS NOT NULL

                AND trip_id IS NOT NULL

                AND stop_id IS NOT NULL

                AND current_stop_sequence
                    IS NOT NULL

                AND vehicle_timestamp
                    IS NOT NULL
        ),

        stop_periods AS (

            SELECT
                vehicle_id,
                trip_id,
                stop_id,
                current_stop_sequence,

                COUNT(*) FILTER (
                    WHERE
                        current_status = 1

                        AND (
                            previous_status IS NULL
                            OR previous_status <> 1
                        )
                ) AS stopped_periods

            FROM ordered

            GROUP BY
                vehicle_id,
                trip_id,
                stop_id,
                current_stop_sequence
        )

        SELECT

            COUNT(*) FILTER (
                WHERE stopped_periods = 1
            ) AS one_period,

            COUNT(*) FILTER (
                WHERE stopped_periods > 1
            ) AS multiple_periods,

            MAX(stopped_periods)
                AS maximum_periods

        FROM stop_periods

        WHERE stopped_periods > 0;
        """,
        (RECENT_HOURS,),
    )

    row = cursor.fetchone()

    print(
        f"Trip-stops with one stopped period:      "
        f"{row[0]:,}"
    )

    print(
        f"Trip-stops with multiple stopped periods: "
        f"{row[1]:,}"
    )

    print(
        f"Maximum stopped periods for one stop:     "
        f"{row[2]}"
    )


def profile_backward_sequences(cursor):
    print_section("STOP SEQUENCE PROGRESSION")

    cursor.execute(
        """
        WITH ordered AS (

            SELECT
                vehicle_id,
                trip_id,
                vehicle_timestamp,
                current_stop_sequence,

                LAG(current_stop_sequence) OVER (
                    PARTITION BY
                        vehicle_id,
                        trip_id

                    ORDER BY vehicle_timestamp
                ) AS previous_stop_sequence

            FROM vehicle_events_v2

            WHERE
                ingestion_timestamp
                    >= NOW() - (%s * INTERVAL '1 hour')

                AND vehicle_id IS NOT NULL

                AND trip_id IS NOT NULL

                AND vehicle_timestamp
                    IS NOT NULL

                AND current_stop_sequence
                    IS NOT NULL
        )

        SELECT

            COUNT(*) FILTER (
                WHERE previous_stop_sequence
                    IS NOT NULL
            ) AS transitions,

            COUNT(*) FILTER (
                WHERE
                    previous_stop_sequence
                        IS NOT NULL

                    AND current_stop_sequence
                        < previous_stop_sequence
            ) AS backward_transitions

        FROM ordered;
        """,
        (RECENT_HOURS,),
    )

    row = cursor.fetchone()

    transitions = row[0]
    backwards = row[1]

    print(
        f"Sequence comparisons:       "
        f"{transitions:,}"
    )

    print(
        f"Backward sequence changes:  "
        f"{backwards:,}"
    )

    if transitions:
        print(
            f"Backward rate:              "
            f"{backwards / transitions * 100:.4f}%"
        )


def profile_observation_gaps(cursor):
    print_section("OBSERVATION GAPS")

    cursor.execute(
        """
        WITH ordered AS (

            SELECT
                vehicle_id,
                trip_id,
                vehicle_timestamp,

                LAG(vehicle_timestamp) OVER (
                    PARTITION BY
                        vehicle_id,
                        trip_id

                    ORDER BY vehicle_timestamp
                ) AS previous_timestamp

            FROM vehicle_events_v2

            WHERE
                ingestion_timestamp
                    >= NOW() - (%s * INTERVAL '1 hour')

                AND vehicle_id IS NOT NULL

                AND trip_id IS NOT NULL

                AND vehicle_timestamp
                    IS NOT NULL
        ),

        gaps AS (

            SELECT

                EXTRACT(
                    EPOCH FROM (
                        vehicle_timestamp
                        - previous_timestamp
                    )
                ) AS gap_seconds

            FROM ordered

            WHERE previous_timestamp
                IS NOT NULL
        )

        SELECT

            AVG(gap_seconds),

            percentile_cont(0.50)
                WITHIN GROUP (
                    ORDER BY gap_seconds
                ),

            percentile_cont(0.95)
                WITHIN GROUP (
                    ORDER BY gap_seconds
                ),

            MAX(gap_seconds)

        FROM gaps

        WHERE gap_seconds > 0

          AND gap_seconds <= 60;
        """,
        (RECENT_HOURS,),
    )

    row = cursor.fetchone()

    print(
        f"Average gap: "
        f"{row[0]:.2f} sec"
        if row[0] is not None
        else "Average gap: None"
    )

    print(
        f"Median gap:  "
        f"{row[1]} sec"
    )

    print(
        f"P95 gap:     "
        f"{row[2]} sec"
    )

    print(
        f"Max <=60s:   "
        f"{row[3]} sec"
    )


def main():

    print(
        f"Profiling realtime stop transitions "
        f"from the last {RECENT_HOURS} hour(s) "
        f"using vehicle_events_v2"
    )

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            profile_status_counts(cursor)

            profile_trip_stop_coverage(cursor)

            profile_multiple_stopped_periods(cursor)

            profile_backward_sequences(cursor)

            profile_observation_gaps(cursor)

    finally:

        connection.close()


    print()

    print("=" * 60)

    print(
        "REALTIME STOP TRANSITION PROFILE COMPLETE"
    )

    print("=" * 60)


if __name__ == "__main__":
    main()