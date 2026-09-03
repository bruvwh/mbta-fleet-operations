from datetime import (
    date,
    datetime,
)

from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import streamlit as st

from google.cloud import bigquery


# ============================================================
# Configuration
# ============================================================

PROJECT_ID = "mbta-fleet-operations-alu"
DATASET_ID = "mbta_analytics"

BOSTON_TIMEZONE = ZoneInfo(
    "America/New_York"
)

SUBWAY_ROUTES = [
    "Red",
    "Orange",
    "Blue",
    "Green-B",
    "Green-C",
    "Green-D",
    "Green-E",
    "Mattapan",
]


TRIP_TABLE = (
    f"{PROJECT_ID}.{DATASET_ID}."
    "mart_realtime_trip_anomalies"
)

ROUTE_DAILY_TABLE = (
    f"{PROJECT_ID}.{DATASET_ID}."
    "mart_realtime_route_daily"
)

ROUTE_HOURLY_TABLE = (
    f"{PROJECT_ID}.{DATASET_ID}."
    "mart_realtime_route_hourly"
)

STOP_SCORE_TABLE = (
    f"{PROJECT_ID}.{DATASET_ID}."
    "int_realtime_stop_anomaly_scores"
)

GTFS_STOPS_TABLE = (
    f"{PROJECT_ID}.{DATASET_ID}."
    "gtfs_stops"
)


# ============================================================
# Streamlit page
# ============================================================

st.set_page_config(
    page_title="MBTA Fleet Operations",
    page_icon="🚇",
    layout="wide",
)


# ============================================================
# BigQuery
# ============================================================

@st.cache_resource
def get_bigquery_client():

    return bigquery.Client(
        project=PROJECT_ID
    )


def dataframe_from_query(
    query,
    query_parameters=None,
):

    client = get_bigquery_client()

    if query_parameters is None:

        query_parameters = []

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters,
        use_query_cache=True,
    )

    rows = (
        client.query(
            query,
            job_config=job_config,
        )
        .result()
    )

    records = [
        dict(row.items())
        for row in rows
    ]

    return pd.DataFrame(
        records
    )


def subway_route_parameter():

    return bigquery.ArrayQueryParameter(
        "subway_routes",
        "STRING",
        SUBWAY_ROUTES,
    )


# ============================================================
# Dashboard queries
# ============================================================

@st.cache_data(
    ttl=120,
    show_spinner=False,
)
def get_service_dates():

    query = f"""
    SELECT DISTINCT
        service_date

    FROM
        `{TRIP_TABLE}`

    WHERE
        service_date IS NOT NULL

        AND route_id IN UNNEST(
            @subway_routes
        )

    ORDER BY
        service_date DESC
    """

    parameters = [
        subway_route_parameter()
    ]

    return dataframe_from_query(
        query,
        parameters,
    )


@st.cache_data(
    ttl=120,
    show_spinner=False,
)
def get_route_daily(
    service_date_string,
):

    query = f"""
    SELECT
        route_id,

        SUM(
            observed_trip_count
        ) AS observed_trip_count,

        SUM(
            evaluable_trip_count
        ) AS evaluable_trip_count,

        SUM(
            sustained_late_trip_count
        ) AS sustained_late_trip_count,

        SUM(
            normal_trip_count
        ) AS normal_trip_count,

        SUM(
            insufficient_coverage_trip_count
        ) AS insufficient_coverage_trip_count,

        100.0
        * SAFE_DIVIDE(

            SUM(
                sustained_late_trip_count
            ),

            SUM(
                evaluable_trip_count
            )

        ) AS sustained_anomaly_rate_percent,

        100.0
        * SAFE_DIVIDE(

            SUM(
                insufficient_coverage_trip_count
            ),

            SUM(
                observed_trip_count
            )

        ) AS insufficient_coverage_rate_percent

    FROM
        `{ROUTE_DAILY_TABLE}`

    WHERE
        service_date = @service_date

        AND route_id IN UNNEST(
            @subway_routes
        )

    GROUP BY
        route_id

    ORDER BY
        sustained_anomaly_rate_percent DESC,
        route_id
    """

    parameters = [

        bigquery.ScalarQueryParameter(
            "service_date",
            "DATE",
            service_date_string,
        ),

        subway_route_parameter(),
    ]

    return dataframe_from_query(
        query,
        parameters,
    )


@st.cache_data(
    ttl=120,
    show_spinner=False,
)
def get_route_hourly(
    service_date_string,
    route_id,
):

    query = f"""
    SELECT
        service_hour,

        direction_id,

        inferred_stop_count,

        observed_stop_count,

        distinct_trip_count,

        anomalous_stop_count,

        certain_late_stop_count,

        possible_late_stop_count,

        anomaly_rate_percent,

        avg_arrival_deviation_seconds,

        route_hour_classification

    FROM
        `{ROUTE_HOURLY_TABLE}`

    WHERE
        service_date = @service_date

        AND route_id = @route_id

    ORDER BY
        service_hour,
        direction_id
    """

    parameters = [

        bigquery.ScalarQueryParameter(
            "service_date",
            "DATE",
            service_date_string,
        ),

        bigquery.ScalarQueryParameter(
            "route_id",
            "STRING",
            route_id,
        ),
    ]

    return dataframe_from_query(
        query,
        parameters,
    )


@st.cache_data(
    ttl=120,
    show_spinner=False,
)
def get_trips(
    service_date_string,
    route_id,
):

    query = f"""
    SELECT
        feed_checksum,

        trip_id,

        direction_id,

        inferred_stop_count,

        observed_stop_count,

        anomalous_stop_count,

        certain_late_stop_count,

        possible_late_stop_count,

        normal_stop_count,

        anomaly_percent,

        avg_arrival_deviation_seconds,

        trip_classification

    FROM
        `{TRIP_TABLE}`

    WHERE
        service_date = @service_date

        AND route_id = @route_id

    ORDER BY

        CASE

            WHEN trip_classification
                = 'SUSTAINED_LATE'

            THEN 1


            WHEN trip_classification
                = 'NORMAL'

            THEN 2


            ELSE 3

        END,

        anomaly_percent DESC,

        trip_id
    """

    parameters = [

        bigquery.ScalarQueryParameter(
            "service_date",
            "DATE",
            service_date_string,
        ),

        bigquery.ScalarQueryParameter(
            "route_id",
            "STRING",
            route_id,
        ),
    ]

    return dataframe_from_query(
        query,
        parameters,
    )


@st.cache_data(
    ttl=120,
    show_spinner=False,
)
def get_trip_stops(
    service_date_string,
    feed_checksum,
    trip_id,
):

    query = f"""
    SELECT
        scores.stop_sequence,

        scores.stop_id,

        COALESCE(
            stops.stop_name,
            scores.stop_id
        ) AS stop_name,

        scores.direction_id,

        scores.scheduled_arrival,

        scores.arrival_estimate,

        scores.arrival_deviation_seconds,

        scores.arrival_deviation_lower_seconds,

        scores.arrival_deviation_upper_seconds,

        scores.arrival_uncertainty_seconds,

        scores.inference_quality,

        scores.schedule_match_anomaly,

        scores.scoring_eligible,

        scores.baseline_level,

        scores.baseline_observation_count,

        scores.baseline_median_seconds,

        scores.upper_anomaly_fence_seconds,

        scores.stop_classification,

        scores.is_anomalous,

        scores.is_certain_late,

        scores.seconds_above_upper_fence

    FROM
        `{STOP_SCORE_TABLE}`
            AS scores

    LEFT JOIN
        `{GTFS_STOPS_TABLE}`
            AS stops

      ON scores.feed_checksum
            = stops.feed_checksum

     AND scores.stop_id
            = stops.stop_id

    WHERE
        scores.service_date
            = @service_date

        AND scores.feed_checksum
            = @feed_checksum

        AND scores.trip_id
            = @trip_id

    ORDER BY
        scores.stop_sequence
    """

    parameters = [

        bigquery.ScalarQueryParameter(
            "service_date",
            "DATE",
            service_date_string,
        ),

        bigquery.ScalarQueryParameter(
            "feed_checksum",
            "STRING",
            feed_checksum,
        ),

        bigquery.ScalarQueryParameter(
            "trip_id",
            "STRING",
            trip_id,
        ),
    ]

    return dataframe_from_query(
        query,
        parameters,
    )


# ============================================================
# Helpers
# ============================================================

def format_number(value):

    if pd.isna(value):

        return "0"

    return f"{int(value):,}"


def format_percent(value):

    if pd.isna(value):

        return "0.00%"

    return f"{float(value):.2f}%"


def get_default_service_date_index(
    service_dates,
):

    today_boston = (
        datetime.now(
            BOSTON_TIMEZONE
        )
        .date()
    )

    for index, service_date in enumerate(
        service_dates
    ):

        if isinstance(
            service_date,
            datetime,
        ):

            candidate_date = (
                service_date.date()
            )

        elif isinstance(
            service_date,
            date,
        ):

            candidate_date = (
                service_date
            )

        else:

            candidate_date = (
                date.fromisoformat(
                    str(service_date)
                )
            )

        if candidate_date < today_boston:

            return index

    return 0


# ============================================================
# Header
# ============================================================

st.title(
    "MBTA Fleet Operations Dashboard"
)

st.caption(
    "Realtime fleet monitoring, service reliability, "
    "and operational anomaly detection."
)

st.caption(
    "Built from MBTA static GTFS schedules, "
    "GTFS-Realtime VehiclePositions observations, "
    "historical LAMP subway performance data, "
    "and BigQuery/dbt analytics models."
)


# ============================================================
# Service date
# ============================================================

service_dates_df = (
    get_service_dates()
)


if service_dates_df.empty:

    st.error(
        "No subway realtime trip data is "
        "available in BigQuery."
    )

    st.stop()


service_dates = (
    service_dates_df[
        "service_date"
    ]
    .tolist()
)


default_service_date_index = (
    get_default_service_date_index(
        service_dates
    )
)


selected_service_date = (
    st.selectbox(
        "Service Date",
        options=service_dates,
        index=default_service_date_index,
    )
)


if isinstance(
    selected_service_date,
    datetime,
):

    selected_date_object = (
        selected_service_date.date()
    )

elif isinstance(
    selected_service_date,
    date,
):

    selected_date_object = (
        selected_service_date
    )

else:

    selected_date_object = (
        date.fromisoformat(
            str(
                selected_service_date
            )
        )
    )


selected_service_date_string = (
    selected_date_object.isoformat()
)


today_boston = (
    datetime.now(
        BOSTON_TIMEZONE
    )
    .date()
)


if selected_date_object == today_boston:

    st.warning(
        "You are viewing the current service day. "
        "Trip counts, coverage, and anomaly rates "
        "are incomplete and will continue changing."
    )


# ============================================================
# Daily route data
# ============================================================

route_daily_df = (
    get_route_daily(
        selected_service_date_string
    )
)


if route_daily_df.empty:

    st.warning(
        "No subway route-level operations are "
        "available for this service date."
    )

    st.stop()


# ============================================================
# System KPIs
# ============================================================

observed_trips = (
    route_daily_df[
        "observed_trip_count"
    ].sum()
)

evaluable_trips = (
    route_daily_df[
        "evaluable_trip_count"
    ].sum()
)

sustained_trips = (
    route_daily_df[
        "sustained_late_trip_count"
    ].sum()
)

insufficient_trips = (
    route_daily_df[
        "insufficient_coverage_trip_count"
    ].sum()
)


system_anomaly_rate = (

    100.0
    * sustained_trips
    / evaluable_trips

    if evaluable_trips > 0

    else 0.0
)


system_insufficient_rate = (

    100.0
    * insufficient_trips
    / observed_trips

    if observed_trips > 0

    else 0.0
)


metric_1, metric_2, metric_3, metric_4 = (
    st.columns(4)
)


metric_1.metric(
    "Observed Trips",
    format_number(
        observed_trips
    ),
)


metric_2.metric(
    "Evaluable Trips",
    format_number(
        evaluable_trips
    ),
)


metric_3.metric(
    "Sustained Anomaly Rate",
    format_percent(
        system_anomaly_rate
    ),
)


metric_4.metric(
    "Insufficient Coverage",
    format_percent(
        system_insufficient_rate
    ),
)


st.caption(
    "Insufficient coverage represents limited "
    "observability, not a confirmed cancellation "
    "or service failure."
)


# ============================================================
# Route performance
# ============================================================

st.subheader(
    "Route Performance"
)

st.markdown(
    "**Sustained Anomaly Rate by Route**"
)


route_chart_df = (
    route_daily_df[
        [
            "route_id",
            "observed_trip_count",
            "evaluable_trip_count",
            "sustained_late_trip_count",
            "sustained_anomaly_rate_percent",
        ]
    ]
    .copy()
)


route_chart = (

    alt.Chart(
        route_chart_df
    )

    .mark_bar()

    .encode(

        x=alt.X(
            "sustained_anomaly_rate_percent:Q",
            title=(
                "Sustained Anomaly Rate (%)"
            ),
            scale=alt.Scale(
                domainMin=0
            ),
        ),

        y=alt.Y(
            "route_id:N",
            title=None,
            sort="-x",
        ),

        tooltip=[

            alt.Tooltip(
                "route_id:N",
                title="Route",
            ),

            alt.Tooltip(
                "observed_trip_count:Q",
                title="Observed Trips",
                format=",",
            ),

            alt.Tooltip(
                "evaluable_trip_count:Q",
                title="Evaluable Trips",
                format=",",
            ),

            alt.Tooltip(
                "sustained_late_trip_count:Q",
                title="Sustained Trips",
                format=",",
            ),

            alt.Tooltip(
                "sustained_anomaly_rate_percent:Q",
                title="Anomaly Rate",
                format=".2f",
            ),
        ],
    )

    .properties(
        height=300
    )
)


st.altair_chart(
    route_chart,
    use_container_width=True,
)


# ============================================================
# Route drilldown
# ============================================================

st.subheader(
    "Route Drilldown"
)


available_routes = (

    route_daily_df[
        "route_id"
    ]
    .dropna()
    .astype(str)
    .tolist()
)


selected_route = (
    st.selectbox(
        "Route",
        options=available_routes,
    )
)


with st.expander(
    "How are anomalies detected?"
):

    st.write(
        "Historical LAMP arrival delays are used "
        "to build route, direction, stop, hour, "
        "and weekend-specific baselines."
    )

    st.write(
        "Realtime stop arrivals are compared "
        "against the historical upper anomaly "
        "fence. The arrival inference interval "
        "is preserved instead of treating the "
        "estimated arrival time as exact."
    )

    st.write(
        "CERTAIN_LATE means the complete inferred "
        "arrival interval is beyond the upper "
        "historical fence. POSSIBLE_LATE means "
        "only part of the interval crosses it."
    )

    st.write(
        "These are statistical operational signals, "
        "not confirmed MBTA incidents."
    )


# ============================================================
# Hourly pattern
# ============================================================

st.markdown(
    "**Hourly Operational Pattern**"
)


hourly_df = (
    get_route_hourly(
        selected_service_date_string,
        selected_route,
    )
)


if hourly_df.empty:

    st.info(
        "No hourly observations are available "
        "for this route."
    )

else:

    hourly_df = (
        hourly_df.copy()
    )

    hourly_df[
        "direction_label"
    ] = (

        "Direction "
        + hourly_df[
            "direction_id"
        ]
        .fillna(-1)
        .astype(int)
        .astype(str)
    )


    hourly_chart = (

        alt.Chart(
            hourly_df
        )

        .mark_line(
            point=True
        )

        .encode(

            x=alt.X(
                "service_hour:O",
                title="Service Hour",
            ),

            y=alt.Y(
                "anomaly_rate_percent:Q",
                title="Anomaly Rate (%)",
                scale=alt.Scale(
                    domainMin=0
                ),
            ),

            color=alt.Color(
                "direction_label:N",
                title="Direction",
            ),

            tooltip=[

                alt.Tooltip(
                    "service_hour:O",
                    title="Hour",
                ),

                alt.Tooltip(
                    "direction_label:N",
                    title="Direction",
                ),

                alt.Tooltip(
                    "observed_stop_count:Q",
                    title="Evaluable Stops",
                    format=",",
                ),

                alt.Tooltip(
                    "distinct_trip_count:Q",
                    title="Trips",
                    format=",",
                ),

                alt.Tooltip(
                    "anomaly_rate_percent:Q",
                    title="Anomaly Rate",
                    format=".2f",
                ),

                alt.Tooltip(
                    "route_hour_classification:N",
                    title="Classification",
                ),
            ],
        )

        .properties(
            height=300
        )
    )


    st.altair_chart(
        hourly_chart,
        use_container_width=True,
    )


    st.caption(
        "Route-hour windows require at least "
        "10 evaluable stop observations and "
        "3 distinct trips before anomaly "
        "classification."
    )


    with st.expander(
        "Hourly Details"
    ):

        hourly_display = (
            hourly_df[
                [
                    "service_hour",
                    "direction_id",
                    "observed_stop_count",
                    "distinct_trip_count",
                    "anomalous_stop_count",
                    "anomaly_rate_percent",
                    "avg_arrival_deviation_seconds",
                    "route_hour_classification",
                ]
            ]
            .rename(
                columns={
                    "service_hour":
                        "Hour",

                    "direction_id":
                        "Direction",

                    "observed_stop_count":
                        "Evaluable Stops",

                    "distinct_trip_count":
                        "Trips",

                    "anomalous_stop_count":
                        "Anomalous Stops",

                    "anomaly_rate_percent":
                        "Anomaly Rate (%)",

                    "avg_arrival_deviation_seconds":
                        "Avg Arrival Deviation (sec)",

                    "route_hour_classification":
                        "Classification",
                }
            )
        )

        st.dataframe(
            hourly_display,
            use_container_width=True,
            hide_index=True,
        )


# ============================================================
# Trip drilldown
# ============================================================

st.subheader(
    "Trip Drilldown"
)


trips_df = (
    get_trips(
        selected_service_date_string,
        selected_route,
    )
)


if trips_df.empty:

    st.info(
        "No trips are available for "
        "this route and service date."
    )

    st.stop()


classification_options = [
    "All",
    "SUSTAINED_LATE",
    "NORMAL",
    "INSUFFICIENT_COVERAGE",
]


selected_classification = (
    st.selectbox(
        "Trip Classification",
        options=classification_options,
    )
)


filtered_trips_df = (
    trips_df.copy()
)


if (
    selected_classification
    != "All"
):

    filtered_trips_df = (
        filtered_trips_df[
            filtered_trips_df[
                "trip_classification"
            ]
            == selected_classification
        ]
        .copy()
    )


trip_display = (

    filtered_trips_df[
        [
            "trip_id",
            "direction_id",
            "observed_stop_count",
            "anomalous_stop_count",
            "anomaly_percent",
            "avg_arrival_deviation_seconds",
            "trip_classification",
        ]
    ]

    .rename(
        columns={
            "trip_id":
                "Trip",

            "direction_id":
                "Direction",

            "observed_stop_count":
                "Evaluable Stops",

            "anomalous_stop_count":
                "Anomalous Stops",

            "anomaly_percent":
                "Anomaly Rate (%)",

            "avg_arrival_deviation_seconds":
                "Avg Arrival Deviation (sec)",

            "trip_classification":
                "Classification",
        }
    )
)


st.dataframe(
    trip_display,
    use_container_width=True,
    hide_index=True,
)


# ============================================================
# Individual trip
# ============================================================

if not filtered_trips_df.empty:

    st.markdown(
        "**Inspect Individual Trip**"
    )


    trip_options_df = (
        filtered_trips_df
        .reset_index(
            drop=True
        )
        .copy()
    )


    trip_options_df[
        "selection_label"
    ] = (

        trip_options_df[
            "trip_id"
        ].astype(str)

        + " | Dir "

        + trip_options_df[
            "direction_id"
        ]
        .fillna(-1)
        .astype(int)
        .astype(str)

        + " | "

        + trip_options_df[
            "trip_classification"
        ].astype(str)
    )


    selected_trip_label = (
        st.selectbox(
            "Trip",
            options=(
                trip_options_df[
                    "selection_label"
                ]
                .tolist()
            ),
        )
    )


    selected_trip_row = (

        trip_options_df[
            trip_options_df[
                "selection_label"
            ]
            == selected_trip_label
        ]

        .iloc[0]
    )


    selected_trip_id = str(
        selected_trip_row[
            "trip_id"
        ]
    )


    selected_feed_checksum = str(
        selected_trip_row[
            "feed_checksum"
        ]
    )


    stops_df = (
        get_trip_stops(
            selected_service_date_string,
            selected_feed_checksum,
            selected_trip_id,
        )
    )


    if stops_df.empty:

        st.info(
            "No stop-level inference is "
            "available for this trip."
        )

    else:

        st.markdown(
            "**Arrival Deviation by Stop**"
        )


        stops_chart_df = (
            stops_df.copy()
        )


        stops_chart = (

            alt.Chart(
                stops_chart_df
            )

            .mark_line(
                point=True
            )

            .encode(

                x=alt.X(
                    "stop_sequence:O",
                    title="Stop Sequence",
                ),

                y=alt.Y(
                    "arrival_deviation_seconds:Q",
                    title=(
                        "Arrival Deviation "
                        "(seconds)"
                    ),
                ),

                color=alt.Color(
                    "stop_classification:N",
                    title="Stop Classification",
                ),

                tooltip=[

                    alt.Tooltip(
                        "stop_sequence:O",
                        title="Sequence",
                    ),

                    alt.Tooltip(
                        "stop_name:N",
                        title="Stop",
                    ),

                    alt.Tooltip(
                        "arrival_deviation_seconds:Q",
                        title="Arrival Deviation",
                        format=".0f",
                    ),

                    alt.Tooltip(
                        "arrival_uncertainty_seconds:Q",
                        title="Uncertainty",
                        format=".0f",
                    ),

                    alt.Tooltip(
                        "stop_classification:N",
                        title="Classification",
                    ),
                ],
            )

            .properties(
                height=300
            )
        )


        zero_line = (

            alt.Chart(
                pd.DataFrame(
                    {
                        "zero": [0]
                    }
                )
            )

            .mark_rule(
                strokeDash=[4, 4]
            )

            .encode(
                y="zero:Q"
            )
        )


        st.altair_chart(
            stops_chart
            + zero_line,
            use_container_width=True,
        )


        st.markdown(
            "**Stop Details**"
        )


        stops_display = (

            stops_df[
                [
                    "stop_sequence",
                    "stop_name",
                    "stop_id",
                    "arrival_deviation_seconds",
                    "arrival_deviation_lower_seconds",
                    "arrival_deviation_upper_seconds",
                    "arrival_uncertainty_seconds",
                    "inference_quality",
                    "baseline_level",
                    "baseline_median_seconds",
                    "upper_anomaly_fence_seconds",
                    "stop_classification",
                ]
            ]

            .rename(
                columns={
                    "stop_sequence":
                        "Sequence",

                    "stop_name":
                        "Stop",

                    "stop_id":
                        "Stop ID",

                    "arrival_deviation_seconds":
                        "Arrival Deviation (sec)",

                    "arrival_deviation_lower_seconds":
                        "Lower Bound (sec)",

                    "arrival_deviation_upper_seconds":
                        "Upper Bound (sec)",

                    "arrival_uncertainty_seconds":
                        "Uncertainty (sec)",

                    "inference_quality":
                        "Inference Quality",

                    "baseline_level":
                        "Baseline",

                    "baseline_median_seconds":
                        "Historical Median (sec)",

                    "upper_anomaly_fence_seconds":
                        "Late Fence (sec)",

                    "stop_classification":
                        "Classification",
                }
            )
        )


        st.dataframe(
            stops_display,
            use_container_width=True,
            hide_index=True,
        )


# ============================================================
# Footer
# ============================================================

st.divider()

st.caption(
    "Analytics are generated from inferred "
    "GTFS-Realtime stop observations and "
    "historical statistical baselines. "
    "Anomaly classifications indicate unusual "
    "operational patterns and should not be "
    "interpreted as confirmed incidents."
)