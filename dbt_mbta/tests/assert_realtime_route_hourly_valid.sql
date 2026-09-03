select *

from {{ ref('mart_realtime_route_hourly') }}

where

    route_hour_classification not in (
        'INSUFFICIENT_COVERAGE',
        'NORMAL',
        'SUSTAINED_LATE'
    )

    or anomaly_rate_percent < 0

    or anomaly_rate_percent > 100

    or (
        route_hour_classification
            = 'INSUFFICIENT_COVERAGE'

        and observed_stop_count >= 10

        and distinct_trip_count >= 3
    )

    or (
        route_hour_classification
            = 'SUSTAINED_LATE'

        and (
            observed_stop_count < 10

            or distinct_trip_count < 3

            or anomaly_rate_percent < 50
        )
    )

    or (
        route_hour_classification = 'NORMAL'

        and observed_stop_count >= 10

        and distinct_trip_count >= 3

        and anomaly_rate_percent >= 50
    )