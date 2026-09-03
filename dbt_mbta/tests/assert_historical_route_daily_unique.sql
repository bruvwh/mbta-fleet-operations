select
    service_date,
    route_id,
    direction_id,
    count(*) as row_count

from {{ ref('mart_historical_route_daily') }}

group by
    service_date,
    route_id,
    direction_id

having count(*) > 1