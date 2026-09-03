select
    *

from {{ ref('mart_historical_stop_baselines') }}

where
       observation_count < 30

    or q1_delay_seconds > median_delay_seconds

    or median_delay_seconds > q3_delay_seconds

    or lower_anomaly_fence_seconds
        > upper_anomaly_fence_seconds

    or iqr_delay_seconds < 0