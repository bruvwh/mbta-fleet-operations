CREATE TABLE IF NOT EXISTS historical_stop_baselines_fallback (

    route_id TEXT NOT NULL,
    direction_id INTEGER NOT NULL,
    stop_id TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL,

    observation_count BIGINT NOT NULL,

    arrival_count BIGINT,
    arrival_median_seconds DOUBLE PRECISION,
    arrival_p25_seconds DOUBLE PRECISION,
    arrival_p75_seconds DOUBLE PRECISION,

    departure_count BIGINT,
    departure_median_seconds DOUBLE PRECISION,
    departure_p25_seconds DOUBLE PRECISION,
    departure_p75_seconds DOUBLE PRECISION,

    dwell_count BIGINT,
    dwell_median_seconds DOUBLE PRECISION,
    dwell_p25_seconds DOUBLE PRECISION,
    dwell_p75_seconds DOUBLE PRECISION,

    travel_count BIGINT,
    travel_median_seconds DOUBLE PRECISION,
    travel_p25_seconds DOUBLE PRECISION,
    travel_p75_seconds DOUBLE PRECISION,

    headway_branch_count BIGINT,
    headway_branch_median_seconds DOUBLE PRECISION,
    headway_branch_p25_seconds DOUBLE PRECISION,
    headway_branch_p75_seconds DOUBLE PRECISION,

    headway_trunk_count BIGINT,
    headway_trunk_median_seconds DOUBLE PRECISION,
    headway_trunk_p25_seconds DOUBLE PRECISION,
    headway_trunk_p75_seconds DOUBLE PRECISION,

    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (
        route_id,
        direction_id,
        stop_id,
        is_weekend
    )
);


WITH baselines AS (

    SELECT
        route_id,
        direction_id,
        stop_id,
        is_weekend,

        COUNT(*) AS observation_count,

        COUNT(arrival_deviation_seconds)
            AS arrival_count,

        percentile_cont(0.50)
            WITHIN GROUP (ORDER BY arrival_deviation_seconds)
            FILTER (WHERE arrival_deviation_seconds IS NOT NULL)
            AS arrival_median_seconds,

        percentile_cont(0.25)
            WITHIN GROUP (ORDER BY arrival_deviation_seconds)
            FILTER (WHERE arrival_deviation_seconds IS NOT NULL)
            AS arrival_p25_seconds,

        percentile_cont(0.75)
            WITHIN GROUP (ORDER BY arrival_deviation_seconds)
            FILTER (WHERE arrival_deviation_seconds IS NOT NULL)
            AS arrival_p75_seconds,


        COUNT(departure_deviation_seconds)
            AS departure_count,

        percentile_cont(0.50)
            WITHIN GROUP (ORDER BY departure_deviation_seconds)
            FILTER (WHERE departure_deviation_seconds IS NOT NULL)
            AS departure_median_seconds,

        percentile_cont(0.25)
            WITHIN GROUP (ORDER BY departure_deviation_seconds)
            FILTER (WHERE departure_deviation_seconds IS NOT NULL)
            AS departure_p25_seconds,

        percentile_cont(0.75)
            WITHIN GROUP (ORDER BY departure_deviation_seconds)
            FILTER (WHERE departure_deviation_seconds IS NOT NULL)
            AS departure_p75_seconds,


        COUNT(dwell_time_seconds)
            AS dwell_count,

        percentile_cont(0.50)
            WITHIN GROUP (ORDER BY dwell_time_seconds)
            FILTER (WHERE dwell_time_seconds IS NOT NULL)
            AS dwell_median_seconds,

        percentile_cont(0.25)
            WITHIN GROUP (ORDER BY dwell_time_seconds)
            FILTER (WHERE dwell_time_seconds IS NOT NULL)
            AS dwell_p25_seconds,

        percentile_cont(0.75)
            WITHIN GROUP (ORDER BY dwell_time_seconds)
            FILTER (WHERE dwell_time_seconds IS NOT NULL)
            AS dwell_p75_seconds,


        COUNT(travel_time_seconds)
            AS travel_count,

        percentile_cont(0.50)
            WITHIN GROUP (ORDER BY travel_time_seconds)
            FILTER (WHERE travel_time_seconds IS NOT NULL)
            AS travel_median_seconds,

        percentile_cont(0.25)
            WITHIN GROUP (ORDER BY travel_time_seconds)
            FILTER (WHERE travel_time_seconds IS NOT NULL)
            AS travel_p25_seconds,

        percentile_cont(0.75)
            WITHIN GROUP (ORDER BY travel_time_seconds)
            FILTER (WHERE travel_time_seconds IS NOT NULL)
            AS travel_p75_seconds,


        COUNT(headway_branch_seconds)
            AS headway_branch_count,

        percentile_cont(0.50)
            WITHIN GROUP (ORDER BY headway_branch_seconds)
            FILTER (WHERE headway_branch_seconds IS NOT NULL)
            AS headway_branch_median_seconds,

        percentile_cont(0.25)
            WITHIN GROUP (ORDER BY headway_branch_seconds)
            FILTER (WHERE headway_branch_seconds IS NOT NULL)
            AS headway_branch_p25_seconds,

        percentile_cont(0.75)
            WITHIN GROUP (ORDER BY headway_branch_seconds)
            FILTER (WHERE headway_branch_seconds IS NOT NULL)
            AS headway_branch_p75_seconds,


        COUNT(headway_trunk_seconds)
            AS headway_trunk_count,

        percentile_cont(0.50)
            WITHIN GROUP (ORDER BY headway_trunk_seconds)
            FILTER (WHERE headway_trunk_seconds IS NOT NULL)
            AS headway_trunk_median_seconds,

        percentile_cont(0.25)
            WITHIN GROUP (ORDER BY headway_trunk_seconds)
            FILTER (WHERE headway_trunk_seconds IS NOT NULL)
            AS headway_trunk_p25_seconds,

        percentile_cont(0.75)
            WITHIN GROUP (ORDER BY headway_trunk_seconds)
            FILTER (WHERE headway_trunk_seconds IS NOT NULL)
            AS headway_trunk_p75_seconds

    FROM historical_baseline_eligible

    WHERE
        route_id IS NOT NULL
        AND direction_id IS NOT NULL
        AND stop_id IS NOT NULL

    GROUP BY
        route_id,
        direction_id,
        stop_id,
        is_weekend
)


INSERT INTO historical_stop_baselines_fallback

SELECT
    route_id,
    direction_id,
    stop_id,
    is_weekend,

    observation_count,

    arrival_count,
    arrival_median_seconds,
    arrival_p25_seconds,
    arrival_p75_seconds,

    departure_count,
    departure_median_seconds,
    departure_p25_seconds,
    departure_p75_seconds,

    dwell_count,
    dwell_median_seconds,
    dwell_p25_seconds,
    dwell_p75_seconds,

    travel_count,
    travel_median_seconds,
    travel_p25_seconds,
    travel_p75_seconds,

    headway_branch_count,
    headway_branch_median_seconds,
    headway_branch_p25_seconds,
    headway_branch_p75_seconds,

    headway_trunk_count,
    headway_trunk_median_seconds,
    headway_trunk_p25_seconds,
    headway_trunk_p75_seconds,

    NOW()

FROM baselines

ON CONFLICT (
    route_id,
    direction_id,
    stop_id,
    is_weekend
)

DO UPDATE SET

    observation_count = EXCLUDED.observation_count,

    arrival_count = EXCLUDED.arrival_count,
    arrival_median_seconds = EXCLUDED.arrival_median_seconds,
    arrival_p25_seconds = EXCLUDED.arrival_p25_seconds,
    arrival_p75_seconds = EXCLUDED.arrival_p75_seconds,

    departure_count = EXCLUDED.departure_count,
    departure_median_seconds = EXCLUDED.departure_median_seconds,
    departure_p25_seconds = EXCLUDED.departure_p25_seconds,
    departure_p75_seconds = EXCLUDED.departure_p75_seconds,

    dwell_count = EXCLUDED.dwell_count,
    dwell_median_seconds = EXCLUDED.dwell_median_seconds,
    dwell_p25_seconds = EXCLUDED.dwell_p25_seconds,
    dwell_p75_seconds = EXCLUDED.dwell_p75_seconds,

    travel_count = EXCLUDED.travel_count,
    travel_median_seconds = EXCLUDED.travel_median_seconds,
    travel_p25_seconds = EXCLUDED.travel_p25_seconds,
    travel_p75_seconds = EXCLUDED.travel_p75_seconds,

    headway_branch_count = EXCLUDED.headway_branch_count,
    headway_branch_median_seconds =
        EXCLUDED.headway_branch_median_seconds,
    headway_branch_p25_seconds =
        EXCLUDED.headway_branch_p25_seconds,
    headway_branch_p75_seconds =
        EXCLUDED.headway_branch_p75_seconds,

    headway_trunk_count = EXCLUDED.headway_trunk_count,
    headway_trunk_median_seconds =
        EXCLUDED.headway_trunk_median_seconds,
    headway_trunk_p25_seconds =
        EXCLUDED.headway_trunk_p25_seconds,
    headway_trunk_p75_seconds =
        EXCLUDED.headway_trunk_p75_seconds,

    updated_at = NOW();