select *

from {{ ref('int_realtime_stop_anomaly_scores') }}

where

    stop_classification not in (
        'NOT_EVALUABLE',
        'NORMAL',
        'POSSIBLE_LATE',
        'CERTAIN_LATE'
    )

    or (
        stop_classification
            = 'CERTAIN_LATE'

        and not (
            arrival_deviation_lower_seconds
                > upper_anomaly_fence_seconds
        )
    )

    or (
        stop_classification
            = 'POSSIBLE_LATE'

        and not (
            arrival_deviation_upper_seconds
                > upper_anomaly_fence_seconds
        )
    )

    or (
        stop_classification
            in (
                'CERTAIN_LATE',
                'POSSIBLE_LATE'
            )

        and is_anomalous = false
    )

    or (
        stop_classification
            = 'NORMAL'

        and is_anomalous = true
    )