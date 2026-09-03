{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=[
        'feed_checksum',
        'service_date',
        'trip_id',
        'stop_sequence',
        'stop_id'
    ],
    partition_by={
        "field": "service_date",
        "data_type": "date"
    },
    cluster_by=[
        "route_id",
        "trip_id",
        "stop_id"
    ]
) }}


-- ============================================================
-- Incremental realtime stop anomaly scoring
--
-- Realtime stop features are compared against historical
-- LAMP-derived baselines.
--
-- Normal incremental runs only rescore stop rows that were
-- recently changed upstream.
--
-- Historical baseline changes should be propagated with an
-- explicit full refresh of this model and downstream marts.
-- ============================================================


with changed_features as (

    select *

    from {{ ref(
        'int_realtime_stop_features'
    ) }}

    {% if is_incremental() %}

    where updated_at >= timestamp_sub(

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


-- ============================================================
-- Primary baseline:
--
-- route
-- + direction
-- + stop
-- + service hour
-- + weekend indicator
-- ============================================================

hourly_baselines as (

    select
        route_id,

        direction_id,

        stop_id,

        service_hour,

        is_weekend,

        observation_count,

        q1_delay_seconds,

        median_delay_seconds,

        q3_delay_seconds,

        iqr_delay_seconds,

        lower_anomaly_fence_seconds,

        upper_anomaly_fence_seconds

    from {{ ref(
        'mart_historical_stop_baselines'
    ) }}

    where baseline_level = 'HOURLY'

),


-- ============================================================
-- Fallback baseline:
--
-- route
-- + direction
-- + stop
-- + weekend indicator
--
-- Service hour is intentionally omitted.
-- ============================================================

fallback_baselines as (

    select
        route_id,

        direction_id,

        stop_id,

        is_weekend,

        observation_count,

        q1_delay_seconds,

        median_delay_seconds,

        q3_delay_seconds,

        iqr_delay_seconds,

        lower_anomaly_fence_seconds,

        upper_anomaly_fence_seconds

    from {{ ref(
        'mart_historical_stop_baselines'
    ) }}

    where baseline_level = 'FALLBACK'

),


-- ============================================================
-- Attach the best available historical baseline.
--
-- Hourly baseline has priority.
-- Fallback is used only when an hourly baseline is unavailable.
-- ============================================================

joined_baselines as (

    select
        features.*,


        case

            when
                hourly.upper_anomaly_fence_seconds
                    is not null

            then 'HOURLY'


            when
                fallback.upper_anomaly_fence_seconds
                    is not null

            then 'FALLBACK'


            else null

        end as baseline_level,


        coalesce(
            hourly.observation_count,
            fallback.observation_count
        ) as baseline_observation_count,


        coalesce(
            hourly.q1_delay_seconds,
            fallback.q1_delay_seconds
        ) as baseline_q1_seconds,


        coalesce(
            hourly.median_delay_seconds,
            fallback.median_delay_seconds
        ) as baseline_median_seconds,


        coalesce(
            hourly.q3_delay_seconds,
            fallback.q3_delay_seconds
        ) as baseline_q3_seconds,


        coalesce(
            hourly.iqr_delay_seconds,
            fallback.iqr_delay_seconds
        ) as baseline_iqr_seconds,


        coalesce(
            hourly.lower_anomaly_fence_seconds,
            fallback.lower_anomaly_fence_seconds
        ) as lower_anomaly_fence_seconds,


        coalesce(
            hourly.upper_anomaly_fence_seconds,
            fallback.upper_anomaly_fence_seconds
        ) as upper_anomaly_fence_seconds


    from changed_features
        as features


    left join hourly_baselines
        as hourly

      on features.route_id
            = hourly.route_id

     and features.direction_id
            = hourly.direction_id

     and features.stop_id
            = hourly.stop_id

     and features.service_hour
            = hourly.service_hour

     and features.is_weekend
            = hourly.is_weekend


    left join fallback_baselines
        as fallback

      on features.route_id
            = fallback.route_id

     and features.direction_id
            = fallback.direction_id

     and features.stop_id
            = fallback.stop_id

     and features.is_weekend
            = fallback.is_weekend

),


-- ============================================================
-- Stop classification
--
-- NOT_EVALUABLE
--   Stop does not satisfy realtime scoring requirements or
--   does not have a usable historical baseline.
--
-- CERTAIN_LATE
--   Even the EARLIEST plausible arrival is beyond the upper
--   anomaly fence.
--
-- POSSIBLE_LATE
--   The uncertainty interval crosses the upper anomaly fence,
--   but the complete interval is not beyond it.
--
-- NORMAL
--   The inferred arrival interval does not cross the upper
--   anomaly fence.
-- ============================================================

classified as (

    select
        *,

        upper_anomaly_fence_seconds
            is not null
            as baseline_found,


        case

            when
                not scoring_eligible

                or upper_anomaly_fence_seconds
                    is null

            then 'NOT_EVALUABLE'


            when
                arrival_deviation_lower_seconds
                    > upper_anomaly_fence_seconds

            then 'CERTAIN_LATE'


            when
                arrival_deviation_upper_seconds
                    > upper_anomaly_fence_seconds

            then 'POSSIBLE_LATE'


            else 'NORMAL'

        end as stop_classification

    from joined_baselines

),


-- ============================================================
-- Final stop-grain anomaly result
-- ============================================================

final_scores as (

    select
        * except (
            updated_at
        ),


        stop_classification in (
            'CERTAIN_LATE',
            'POSSIBLE_LATE'
        ) as is_anomalous,


        stop_classification
            = 'CERTAIN_LATE'
            as is_certain_late,


        case

            when
                arrival_deviation_seconds
                    is not null

                and upper_anomaly_fence_seconds
                    is not null

            then
                arrival_deviation_seconds
                - upper_anomaly_fence_seconds

        end as seconds_above_upper_fence,


        current_timestamp()
            as updated_at

    from classified

)


select *

from final_scores