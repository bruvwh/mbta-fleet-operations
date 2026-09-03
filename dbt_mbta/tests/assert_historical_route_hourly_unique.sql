select
    service_date,
    route_id,
    direction_id,
    service_hour,
    count(*) as row_count

from {{ ref('mart_historical_route_hourly') }}

group by
    service_date,
    route_id,
    direction_id,
    service_hour

having count(*) > 1