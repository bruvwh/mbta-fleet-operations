{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=[
        'service_date',
        'route_id',
        'direction_id',
        'service_hour'
    ],
    partition_by={
        "field": "service_date",
        "data_type": "date"
    },
    cluster_by=[
        "route_id",
        "service_hour"
    ]
) }}


with affected_route_hours as (

    select distinct
        service_date,

        route_id,

        direction_id,

        service_hour

    from {{ ref(
        'int_realtime_stop_anomaly_scores'
    ) }}

    where
        route_id is not null

        and service_hour is not null


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


affected_scores as (

    select
        scores.*

    from {{ ref(
        'int_realtime_stop_anomaly_scores'
    ) }} as scores

    join affected_route_hours as affected

      on scores.service_date
            = affected.service_date

     and scores.route_id
            = affected.route_id

     and scores.direction_id
            = affected.direction_id

     and scores.service_hour
            = affected.service_hour

),


route_hour_summary as (

    select
        service_date,

        route_id,

        direction_id,

        service_hour,


        count(*)
            as inferred_stop_count,


        countif(
            stop_classification
                != 'NOT_EVALUABLE'
        ) as observed_stop_count,


        count(
            distinct if(
                stop_classification
                    != 'NOT_EVALUABLE',

                trip_id,

                null
            )
        ) as distinct_trip_count,


        countif(
            is_anomalous
        ) as anomalous_stop_count,


        countif(
            stop_classification
                = 'CERTAIN_LATE'
        ) as certain_late_stop_count,


        countif(
            stop_classification
                = 'POSSIBLE_LATE'
        ) as possible_late_stop_count,


        100.0
        * safe_divide(

            countif(
                is_anomalous
            ),

            countif(
                stop_classification
                    != 'NOT_EVALUABLE'
            )

        ) as anomaly_rate_percent,


        avg(

            if(
                stop_classification
                    != 'NOT_EVALUABLE',

                arrival_deviation_seconds,

                null
            )

        ) as avg_arrival_deviation_seconds

    from affected_scores

    group by
        service_date,
        route_id,
        direction_id,
        service_hour

),


classified as (

    select
        *,


        case

            when
                observed_stop_count < 10

                or distinct_trip_count < 3

            then 'INSUFFICIENT_COVERAGE'


            when anomaly_rate_percent >= 50

            then 'SUSTAINED_LATE'


            else 'NORMAL'

        end as route_hour_classification,


        current_timestamp()
            as updated_at

    from route_hour_summary

)


select *

from classified