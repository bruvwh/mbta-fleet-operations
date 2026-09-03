{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=[
        'service_date',
        'route_id',
        'direction_id'
    ],
    partition_by={
        "field": "service_date",
        "data_type": "date"
    },
    cluster_by=[
        "route_id"
    ]
) }}


with affected_route_days as (

    select distinct
        service_date,

        route_id,

        direction_id

    from {{ ref(
        'mart_realtime_trip_anomalies'
    ) }}

    where route_id is not null


    {% if is_incremental() %}

        and updated_at >= timestamp_sub(

            coalesce(

                (
                    select
                        max(updated_at)

                    from {{ this }}
                ),

                timestamp(
                    '1970-01-01 00:00:00+00'
                )
            ),

            interval 30 minute
        )

    {% endif %}

),


affected_trips as (

    select
        trips.*

    from {{ ref(
        'mart_realtime_trip_anomalies'
    ) }} as trips

    join affected_route_days as affected

      on trips.service_date
            = affected.service_date

     and trips.route_id
            = affected.route_id

     and trips.direction_id
            = affected.direction_id

),


route_daily as (

    select
        service_date,

        route_id,

        direction_id,


        count(*)
            as observed_trip_count,


        countif(
            trip_classification
                != 'INSUFFICIENT_COVERAGE'
        ) as evaluable_trip_count,


        countif(
            trip_classification
                = 'SUSTAINED_LATE'
        ) as sustained_late_trip_count,


        countif(
            trip_classification
                = 'NORMAL'
        ) as normal_trip_count,


        countif(
            trip_classification
                = 'INSUFFICIENT_COVERAGE'
        ) as insufficient_coverage_trip_count,


        100.0
        * safe_divide(

            countif(
                trip_classification
                    = 'SUSTAINED_LATE'
            ),

            countif(
                trip_classification
                    != 'INSUFFICIENT_COVERAGE'
            )

        ) as sustained_anomaly_rate_percent,


        100.0
        * safe_divide(

            countif(
                trip_classification
                    = 'INSUFFICIENT_COVERAGE'
            ),

            count(*)

        ) as insufficient_coverage_rate_percent,


        current_timestamp()
            as updated_at

    from affected_trips

    group by
        service_date,
        route_id,
        direction_id

)


select *

from route_daily