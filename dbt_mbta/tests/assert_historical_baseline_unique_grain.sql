select
    baseline_level,
    route_id,
    direction_id,
    stop_id,
    service_hour,
    is_weekend,
    count(*) as row_count

from {{ ref('mart_historical_stop_baselines') }}

group by
    baseline_level,
    route_id,
    direction_id,
    stop_id,
    service_hour,
    is_weekend

having count(*) > 1