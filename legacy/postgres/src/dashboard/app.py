import altair as alt
import pandas as pd
import psycopg
import streamlit as st


# ============================================================
# Page configuration
# ============================================================

st.set_page_config(
    page_title="MBTA Fleet Operations",
    page_icon="🚇",
    layout="wide",
)


# ============================================================
# Database
# ============================================================

@st.cache_resource
def get_connection():

    return psycopg.connect(
        host=st.secrets["postgres"]["host"],
        port=st.secrets["postgres"]["port"],
        dbname=st.secrets["postgres"]["dbname"],
        user=st.secrets["postgres"]["user"],
        password=st.secrets["postgres"]["password"],
        autocommit=True,
    )


connection = get_connection()


def fetch_dataframe(query, params=None):

    with connection.cursor() as cursor:

        cursor.execute(
            query,
            params or (),
        )

        rows = cursor.fetchall()

        columns = [
            description.name
            for description in cursor.description
        ]

    return pd.DataFrame(
        rows,
        columns=columns,
    )


def convert_numeric(dataframe, columns):

    for column in columns:

        if column in dataframe.columns:

            dataframe[column] = pd.to_numeric(
                dataframe[column],
                errors="coerce",
            )

    return dataframe


def readable_status(value):

    if pd.isna(value):
        return value

    return (
        str(value)
        .replace("_", " ")
        .title()
    )


def format_hour(hour):

    if pd.isna(hour):
        return ""

    hour = int(hour)

    suffix = (
        "AM"
        if hour < 12
        else "PM"
    )

    display_hour = hour % 12

    if display_hour == 0:
        display_hour = 12

    return f"{display_hour} {suffix}"


def get_direction_labels(
    service_date,
    route_id,
):

    direction_df = fetch_dataframe(
        """
        WITH headsign_counts AS (

            SELECT
                instances.direction_id,
                trips.trip_headsign,
                COUNT(*) AS trip_count

            FROM gtfs_trip_instances AS instances

            JOIN gtfs_trips AS trips
              ON instances.feed_checksum =
                    trips.feed_checksum

             AND instances.trip_id =
                    trips.trip_id

            WHERE
                instances.service_date = %s

                AND instances.route_id = %s

                AND trips.trip_headsign IS NOT NULL

                AND TRIM(trips.trip_headsign) <> ''

            GROUP BY
                instances.direction_id,
                trips.trip_headsign
        ),

        ranked_headsigns AS (

            SELECT
                *,

                ROW_NUMBER() OVER (

                    PARTITION BY direction_id

                    ORDER BY
                        trip_count DESC,
                        trip_headsign

                ) AS headsign_rank

            FROM headsign_counts
        )

        SELECT
            direction_id,
            trip_headsign

        FROM ranked_headsigns

        WHERE headsign_rank = 1;
        """,
        (
            service_date,
            route_id,
        ),
    )


    labels = {}


    for _, row in direction_df.iterrows():

        direction_id = row["direction_id"]
        headsign = row["trip_headsign"]

        labels[direction_id] = (
            f"{headsign} "
            f"(Dir {direction_id})"
        )


    return labels


# ============================================================
# Header
# ============================================================

st.title(
    "MBTA Fleet Operations Dashboard"
)

st.write(
    "Realtime fleet monitoring, service reliability, "
    "and operational anomaly detection."
)

st.caption(
    "Built from MBTA static GTFS schedules, realtime "
    "VehiclePositions observations, and historical LAMP "
    "subway performance data."
)


# ============================================================
# Service date selector
#
# All collected dates remain selectable.
# Default to the newest date with evaluable trips.
# ============================================================

date_df = fetch_dataframe(
    """
    SELECT
        service_date,

        SUM(observed_trips)
            AS observed_trips,

        SUM(evaluable_trips)
            AS evaluable_trips

    FROM route_daily_operations

    GROUP BY service_date

    ORDER BY service_date DESC;
    """
)


if date_df.empty:

    st.error(
        "No route-level operational data is available."
    )

    st.stop()


default_date_index = 0


for index, row in date_df.iterrows():

    if (row["evaluable_trips"] or 0) > 0:

        default_date_index = index

        break


selected_date = st.selectbox(
    "Service Date",
    date_df["service_date"].tolist(),
    index=default_date_index,
)


# ============================================================
# Daily KPIs
# ============================================================

kpi_df = fetch_dataframe(
    """
    SELECT

        SUM(observed_trips)
            AS observed_trips,

        SUM(evaluable_trips)
            AS evaluable_trips,

        100.0
        * SUM(
            sustained_late_trips
            + sustained_early_trips
        )
        / NULLIF(
            SUM(evaluable_trips),
            0
        )
            AS sustained_anomaly_pct,

        100.0
        * SUM(
            insufficient_coverage_trips
        )
        / NULLIF(
            SUM(observed_trips),
            0
        )
            AS insufficient_coverage_pct

    FROM route_daily_operations

    WHERE service_date = %s;
    """,
    (selected_date,),
)


kpi_df = convert_numeric(
    kpi_df,
    [
        "observed_trips",
        "evaluable_trips",
        "sustained_anomaly_pct",
        "insufficient_coverage_pct",
    ],
)


kpis = kpi_df.iloc[0]


observed_trips = int(
    kpis["observed_trips"]
    if pd.notna(kpis["observed_trips"])
    else 0
)

evaluable_trips = int(
    kpis["evaluable_trips"]
    if pd.notna(kpis["evaluable_trips"])
    else 0
)

sustained_anomaly_pct = (
    kpis["sustained_anomaly_pct"]
    if pd.notna(
        kpis["sustained_anomaly_pct"]
    )
    else None
)

insufficient_coverage_pct = (
    kpis["insufficient_coverage_pct"]
    if pd.notna(
        kpis["insufficient_coverage_pct"]
    )
    else None
)


# ============================================================
# KPI cards
# ============================================================

col1, col2, col3, col4 = st.columns(4)


with col1:

    st.metric(
        "Observed Trips",
        f"{observed_trips:,}",
        help=(
            "Scheduled trips for which matched realtime "
            "vehicle observations were collected."
        ),
    )


with col2:

    st.metric(
        "Evaluable Trips",
        f"{evaluable_trips:,}",
        help=(
            "Observed trips with enough stop-level "
            "information for trip anomaly classification."
        ),
    )


with col3:

    st.metric(
        "Sustained Anomaly Rate",

        (
            f"{sustained_anomaly_pct:.2f}%"
            if sustained_anomaly_pct is not None
            else "N/A"
        ),

        help=(
            "Percentage of evaluable trips classified as "
            "sustained late or sustained early."
        ),
    )


with col4:

    st.metric(
        "Insufficient Coverage",

        (
            f"{insufficient_coverage_pct:.2f}%"
            if insufficient_coverage_pct is not None
            else "N/A"
        ),

        help=(
            "Percentage of observed trips that did not have "
            "enough stop observations for reliable "
            "classification."
        ),
    )


# ============================================================
# Coverage warning
# ============================================================

if observed_trips == 0:

    st.warning(
        "No realtime trips were observed on this service date."
    )


elif evaluable_trips == 0:

    st.warning(
        "Insufficient realtime coverage to evaluate "
        "service performance for this date."
    )


elif (
    insufficient_coverage_pct is not None
    and insufficient_coverage_pct >= 50
):

    st.warning(
        "Limited realtime coverage: more than half of "
        "observed trips could not be reliably evaluated. "
        "Interpret anomaly metrics cautiously."
    )


st.caption(
    "Insufficient coverage represents limited observability, "
    "not a confirmed cancellation or service failure."
)


# ============================================================
# Route performance
# ============================================================

st.divider()

st.header(
    "Route Performance"
)


route_df = fetch_dataframe(
    """
    SELECT
        route_id,
        observed_trips,
        evaluable_trips,
        sustained_anomaly_pct,
        insufficient_coverage_pct

    FROM route_daily_operations

    WHERE service_date = %s

    ORDER BY
        sustained_anomaly_pct DESC NULLS LAST,
        route_id;
    """,
    (selected_date,),
)


route_df = route_df.rename(
    columns={
        "route_id":
            "Route",

        "observed_trips":
            "Observed Trips",

        "evaluable_trips":
            "Evaluable Trips",

        "sustained_anomaly_pct":
            "Sustained Anomaly Rate",

        "insufficient_coverage_pct":
            "Insufficient Coverage",
    }
)


route_df = convert_numeric(
    route_df,
    [
        "Observed Trips",
        "Evaluable Trips",
        "Sustained Anomaly Rate",
        "Insufficient Coverage",
    ],
)


# ============================================================
# Route table
# ============================================================

st.dataframe(
    route_df,
    use_container_width=True,
    hide_index=True,

    column_config={

        "Observed Trips":
            st.column_config.NumberColumn(
                format="%d"
            ),

        "Evaluable Trips":
            st.column_config.NumberColumn(
                format="%d"
            ),

        "Sustained Anomaly Rate":
            st.column_config.NumberColumn(
                format="%.2f%%"
            ),

        "Insufficient Coverage":
            st.column_config.NumberColumn(
                format="%.2f%%"
            ),
    },
)


# ============================================================
# Route chart
# ============================================================

st.subheader(
    "Sustained Anomaly Rate by Route"
)


valid_route_chart_df = (
    route_df
    .dropna(
        subset=[
            "Sustained Anomaly Rate"
        ]
    )
    .copy()
)


if not valid_route_chart_df.empty:

    maximum_route_rate = (
        valid_route_chart_df[
            "Sustained Anomaly Rate"
        ]
        .max()
    )


    route_axis_max = min(
        100,
        max(
            10,
            maximum_route_rate * 1.25,
        ),
    )


    route_chart = (
        alt.Chart(
            valid_route_chart_df
        )

        .mark_bar()

        .encode(

            x=alt.X(
                "Sustained Anomaly Rate:Q",

                title=(
                    "Sustained Anomaly Rate (%)"
                ),

                scale=alt.Scale(
                    domain=[
                        0,
                        route_axis_max,
                    ]
                ),
            ),

            y=alt.Y(
                "Route:N",
                sort="-x",
                title=None,
            ),

            tooltip=[

                alt.Tooltip(
                    "Route:N",
                    title="Route",
                ),

                alt.Tooltip(
                    "Observed Trips:Q",
                    title="Observed Trips",
                ),

                alt.Tooltip(
                    "Evaluable Trips:Q",
                    title="Evaluable Trips",
                ),

                alt.Tooltip(
                    "Sustained Anomaly Rate:Q",
                    title="Anomaly Rate (%)",
                    format=".2f",
                ),

                alt.Tooltip(
                    "Insufficient Coverage:Q",
                    title=(
                        "Insufficient Coverage (%)"
                    ),
                    format=".2f",
                ),
            ],
        )

        .properties(
            height=300,
        )
    )


    st.altair_chart(
        route_chart,
        use_container_width=True,
    )


else:

    st.info(
        "No routes have sufficient coverage for "
        "anomaly evaluation on this service date."
    )


# ============================================================
# Route drilldown
# ============================================================

st.divider()

st.header(
    "Route Drilldown"
)


route_options = (
    route_df["Route"]
    .dropna()
    .tolist()
)


if not route_options:

    st.info(
        "No routes are available for this service date."
    )

    st.stop()


# Because route_df is sorted by anomaly rate,
# this defaults to the most anomalous evaluable route.

selected_route = st.selectbox(
    "Route",
    route_options,
)


direction_labels = get_direction_labels(
    selected_date,
    selected_route,
)


# ============================================================
# Methodology
# ============================================================

with st.expander(
    "How are anomalies detected?"
):

    st.markdown(
        """
**Stop-level baseline**

Realtime arrival performance is compared with historical
behavior for the same route, direction, stop, service hour,
and weekday/weekend context.

Historical medians and interquartile ranges (IQR) are used
instead of means and standard deviations so the reference
distribution is more resistant to extreme service disruptions.

A primary baseline requires at least 30 historical
observations. If the hour-specific baseline is too sparse,
the system falls back to the same route, direction, stop,
and weekday/weekend context without service hour.

**Stop anomaly**

Tukey-style 1.5 × IQR fences are used as an interpretable
anomaly threshold.

Realtime arrival uncertainty is retained rather than treating
an inferred arrival as exact. This separates possible anomalies
from anomalies whose uncertainty interval is entirely beyond
the historical fence.

**Trip anomaly**

A trip is considered sustained anomalous when it has at least
5 observed stops, at least 3 anomalous stops, and at least
50% of evaluated stops are anomalous.

**Route-hour sample requirement**

Route-hour windows with fewer than 10 stop observations or
fewer than 3 distinct trips are labeled insufficient coverage.

These are statistical operational anomalies, not confirmed
MBTA incidents.
        """
    )


# ============================================================
# Hourly route data
# ============================================================

hourly_df = fetch_dataframe(
    """
    SELECT
        direction_id,
        service_hour,
        observed_stop_events,
        observed_trips,

        certain_late_pct,
        certain_early_pct,
        anomaly_stop_pct,

        median_arrival_deviation_seconds,
        historical_median_seconds,

        median_abs_anomaly_score,
        median_arrival_uncertainty_seconds,

        route_hour_status

    FROM route_hour_anomaly_classification

    WHERE
        service_date = %s
        AND route_id = %s

    ORDER BY
        service_hour,
        direction_id;
    """,
    (
        selected_date,
        selected_route,
    ),
)


hourly_df = hourly_df.rename(
    columns={
        "direction_id":
            "Direction ID",

        "service_hour":
            "Hour",

        "observed_stop_events":
            "Stop Observations",

        "observed_trips":
            "Trips",

        "certain_late_pct":
            "Certain Late %",

        "certain_early_pct":
            "Certain Early %",

        "anomaly_stop_pct":
            "Anomaly %",

        "median_arrival_deviation_seconds":
            "Median Arrival Deviation",

        "historical_median_seconds":
            "Historical Baseline",

        "median_abs_anomaly_score":
            "Median Anomaly Score",

        "median_arrival_uncertainty_seconds":
            "Median Uncertainty",

        "route_hour_status":
            "Status",
    }
)


# ============================================================
# Hourly presentation
# ============================================================

st.subheader(
    "Hourly Operational Pattern"
)


if not hourly_df.empty:

    hourly_df = convert_numeric(
        hourly_df,
        [
            "Hour",
            "Stop Observations",
            "Trips",
            "Certain Late %",
            "Certain Early %",
            "Anomaly %",
            "Median Arrival Deviation",
            "Historical Baseline",
            "Median Anomaly Score",
            "Median Uncertainty",
        ],
    )


    hourly_df["Direction"] = (
        hourly_df["Direction ID"]
        .apply(
            lambda direction_id:
                direction_labels.get(
                    direction_id,
                    f"Direction {direction_id}",
                )
        )
    )


    hourly_df["Status"] = (
        hourly_df["Status"]
        .apply(readable_status)
    )


    hourly_df["Hour Label"] = (
        hourly_df["Hour"]
        .apply(format_hour)
    )


    # --------------------------------------------------------
    # Heatmap
    # --------------------------------------------------------

    heatmap_df = (
        hourly_df.copy()
    )


    heatmap_df["Chart Anomaly %"] = (
        heatmap_df["Anomaly %"]
        .where(
            heatmap_df["Status"]
            != "Insufficient Coverage"
        )
    )


    valid_heatmap_df = (
        heatmap_df
        .dropna(
            subset=["Chart Anomaly %"]
        )
    )


    if not valid_heatmap_df.empty:

        ordered_hours = [

            format_hour(hour)

            for hour in sorted(
                hourly_df[
                    "Hour"
                ]
                .dropna()
                .astype(int)
                .unique()
            )
        ]


        heatmap = (
            alt.Chart(
                valid_heatmap_df
            )

            .mark_rect()

            .encode(

                x=alt.X(
                    "Hour Label:N",
                    title="Service Hour",
                    sort=ordered_hours,
                ),

                y=alt.Y(
                    "Direction:N",
                    title=None,
                ),

                color=alt.Color(
                    "Chart Anomaly %:Q",
                    title="Anomaly Rate (%)",

                    scale=alt.Scale(
                        domain=[0, 100]
                    ),
                ),

                tooltip=[

                    alt.Tooltip(
                        "Hour Label:N",
                        title="Hour",
                    ),

                    alt.Tooltip(
                        "Direction:N",
                        title="Direction",
                    ),

                    alt.Tooltip(
                        "Status:N",
                        title="Status",
                    ),

                    alt.Tooltip(
                        "Stop Observations:Q",
                        title="Stop Observations",
                    ),

                    alt.Tooltip(
                        "Trips:Q",
                        title="Trips",
                    ),

                    alt.Tooltip(
                        "Anomaly %:Q",
                        title="Anomaly Rate (%)",
                        format=".2f",
                    ),

                    alt.Tooltip(
                        "Certain Late %:Q",
                        title="Certain Late (%)",
                        format=".2f",
                    ),

                    alt.Tooltip(
                        "Certain Early %:Q",
                        title="Certain Early (%)",
                        format=".2f",
                    ),

                    alt.Tooltip(
                        "Median Arrival Deviation:Q",
                        title="Median Delay (sec)",
                        format=".1f",
                    ),

                    alt.Tooltip(
                        "Historical Baseline:Q",
                        title=(
                            "Historical Median (sec)"
                        ),
                        format=".1f",
                    ),

                    alt.Tooltip(
                        "Median Uncertainty:Q",
                        title=(
                            "Median Uncertainty (sec)"
                        ),
                        format=".1f",
                    ),
                ],
            )

            .properties(
                height=180,
            )
        )


        st.altair_chart(
            heatmap,
            use_container_width=True,
        )


        st.caption(
            "Blank route-hour windows do not meet the "
            "minimum sample requirement for anomaly "
            "classification."
        )


    else:

        st.info(
            "No hourly windows have enough observations "
            "for anomaly evaluation for this route and date."
        )


    # --------------------------------------------------------
    # Hourly table
    # --------------------------------------------------------

    st.subheader(
        "Hourly Details"
    )


    display_hourly_df = (
        hourly_df[
            [
                "Hour Label",
                "Direction",
                "Stop Observations",
                "Trips",
                "Anomaly %",
                "Certain Late %",
                "Certain Early %",
                "Median Arrival Deviation",
                "Historical Baseline",
                "Median Uncertainty",
                "Status",
            ]
        ]
        .rename(
            columns={
                "Hour Label":
                    "Hour",
            }
        )
    )


    st.dataframe(
        display_hourly_df,
        use_container_width=True,
        hide_index=True,

        column_config={

            "Anomaly %":
                st.column_config.NumberColumn(
                    format="%.2f%%"
                ),

            "Certain Late %":
                st.column_config.NumberColumn(
                    format="%.2f%%"
                ),

            "Certain Early %":
                st.column_config.NumberColumn(
                    format="%.2f%%"
                ),

            "Median Arrival Deviation":
                st.column_config.NumberColumn(
                    "Median Delay (sec)",
                    format="%.1f",
                ),

            "Historical Baseline":
                st.column_config.NumberColumn(
                    "Historical Median (sec)",
                    format="%.1f",
                ),

            "Median Uncertainty":
                st.column_config.NumberColumn(
                    "Median Uncertainty (sec)",
                    format="%.1f",
                ),
        },
    )


else:

    st.info(
        "No hourly operational data is available "
        "for this route and service date."
    )


# ============================================================
# Trip drilldown
# ============================================================

st.divider()

st.header(
    "Trip Drilldown"
)


trip_df = fetch_dataframe(
    """
    SELECT
        classification.trip_id,
        classification.direction_id,

        trips.trip_headsign,

        classification.observed_stop_count,
        classification.anomalous_stop_count,
        classification.anomaly_stop_pct,

        classification.certain_late_stop_count,
        classification.certain_early_stop_count,

        classification.max_late_anomaly_score,
        classification.max_early_anomaly_score,

        classification.trip_anomaly_status

    FROM trip_anomaly_classification
        AS classification

    LEFT JOIN gtfs_trips AS trips

      ON classification.feed_checksum =
            trips.feed_checksum

     AND classification.trip_id =
            trips.trip_id

    WHERE
        classification.service_date = %s
        AND classification.route_id = %s;
    """,
    (
        selected_date,
        selected_route,
    ),
)


if not trip_df.empty:

    trip_df = convert_numeric(
        trip_df,
        [
            "observed_stop_count",
            "anomalous_stop_count",
            "anomaly_stop_pct",
            "certain_late_stop_count",
            "certain_early_stop_count",
            "max_late_anomaly_score",
            "max_early_anomaly_score",
        ],
    )


    trip_df["trip_id"] = (
        trip_df["trip_id"]
        .astype(str)
    )


    trip_df["Classification"] = (
        trip_df[
            "trip_anomaly_status"
        ]
        .apply(readable_status)
    )


    def build_trip_direction(row):

        direction_id = (
            row["direction_id"]
        )

        headsign = (
            row["trip_headsign"]
        )


        if (
            pd.notna(headsign)
            and str(headsign).strip()
        ):

            return (
                f"{headsign} "
                f"(Dir {direction_id})"
            )


        return direction_labels.get(
            direction_id,
            f"Direction {direction_id}",
        )


    trip_df["Direction"] = (
        trip_df.apply(
            build_trip_direction,
            axis=1,
        )
    )


    display_trip_df = (
        trip_df[
            [
                "trip_id",
                "Direction",
                "observed_stop_count",
                "anomalous_stop_count",
                "anomaly_stop_pct",
                "certain_late_stop_count",
                "certain_early_stop_count",
                "Classification",
            ]
        ]
        .rename(
            columns={
                "trip_id":
                    "Trip ID",

                "observed_stop_count":
                    "Observed Stops",

                "anomalous_stop_count":
                    "Anomalous Stops",

                "anomaly_stop_pct":
                    "Anomaly Rate",

                "certain_late_stop_count":
                    "Certain Late Stops",

                "certain_early_stop_count":
                    "Certain Early Stops",
            }
        )
    )


    # --------------------------------------------------------
    # Classification filter
    # --------------------------------------------------------

    classification_options = (
        ["All"]
        + sorted(
            display_trip_df[
                "Classification"
            ]
            .dropna()
            .unique()
            .tolist()
        )
    )


    selected_classification = (
        st.selectbox(
            "Trip Classification",
            classification_options,
        )
    )


    filtered_trip_df = (
        display_trip_df.copy()
    )


    if (
        selected_classification
        != "All"
    ):

        filtered_trip_df = (
            filtered_trip_df[
                filtered_trip_df[
                    "Classification"
                ]
                == selected_classification
            ]
        )


    # --------------------------------------------------------
    # Sort useful anomalies first
    # --------------------------------------------------------

    classification_priority = {

        "Sustained Late": 1,

        "Sustained Early": 2,

        "Isolated Or Weak": 3,

        "Normal": 4,

        "Insufficient Coverage": 5,
    }


    filtered_trip_df[
        "Sort Priority"
    ] = (
        filtered_trip_df[
            "Classification"
        ]
        .map(
            classification_priority
        )
        .fillna(6)
    )


    filtered_trip_df = (
        filtered_trip_df

        .sort_values(
            by=[
                "Sort Priority",
                "Anomaly Rate",
                "Observed Stops",
            ],

            ascending=[
                True,
                False,
                False,
            ],
        )

        .drop(
            columns=[
                "Sort Priority"
            ]
        )
    )


    # --------------------------------------------------------
    # Trip table
    # --------------------------------------------------------

    if not filtered_trip_df.empty:

        st.dataframe(
            filtered_trip_df,
            use_container_width=True,
            hide_index=True,

            column_config={

                "Anomaly Rate":
                    st.column_config.NumberColumn(
                        format="%.2f%%"
                    ),
            },
        )


        # ====================================================
        # Individual trip selector
        # ====================================================

        st.subheader(
            "Inspect Individual Trip"
        )


        trip_options = (
            filtered_trip_df[
                "Trip ID"
            ]
            .astype(str)
            .tolist()
        )


        trip_label_map = {}


        for _, row in (
            filtered_trip_df
            .iterrows()
        ):

            trip_id = str(
                row["Trip ID"]
            )

            trip_label_map[
                trip_id
            ] = (
                f"{trip_id} | "
                f"{row['Direction']} | "
                f"{row['Classification']} | "
                f"{row['Anomaly Rate']:.1f}% anomalous"
            )


        selected_trip = (
            st.selectbox(

                "Trip",

                trip_options,

                format_func=(
                    lambda trip_id:
                        trip_label_map.get(
                            trip_id,
                            trip_id,
                        )
                ),

                key="selected_trip",
            )
        )


        selected_trip_row = (
            filtered_trip_df[
                filtered_trip_df[
                    "Trip ID"
                ].astype(str)
                == str(selected_trip)
            ]
            .iloc[0]
        )


        # ----------------------------------------------------
        # Selected trip summary
        # ----------------------------------------------------

        trip_col1, trip_col2, trip_col3, trip_col4 = (
            st.columns(4)
        )


        with trip_col1:

            st.metric(
                "Classification",
                selected_trip_row[
                    "Classification"
                ],
            )


        with trip_col2:

            st.metric(
                "Observed Stops",
                int(
                    selected_trip_row[
                        "Observed Stops"
                    ]
                ),
            )


        with trip_col3:

            st.metric(
                "Anomalous Stops",
                int(
                    selected_trip_row[
                        "Anomalous Stops"
                    ]
                ),
            )


        with trip_col4:

            st.metric(
                "Trip Anomaly Rate",
                (
                    f"{selected_trip_row['Anomaly Rate']:.2f}%"
                ),
            )


        # ====================================================
        # Stop-level anomaly data
        # ====================================================

        stop_df = fetch_dataframe(
            """
            SELECT
                scores.stop_sequence,
                scores.stop_id,

                COALESCE(
                    parent_stop.stop_name,
                    child_stop.stop_name,
                    scores.stop_id
                )
                    AS station_name,

                scores.arrival_deviation_seconds,
                scores.arrival_uncertainty_seconds,

                scores.baseline_type,
                scores.baseline_count,

                scores.anomaly_score,
                scores.anomaly_status

            FROM realtime_stop_anomaly_scores
                AS scores

            LEFT JOIN gtfs_stops AS child_stop

              ON scores.feed_checksum =
                    child_stop.feed_checksum

             AND scores.stop_id =
                    child_stop.stop_id

            LEFT JOIN gtfs_stops AS parent_stop

              ON child_stop.feed_checksum =
                    parent_stop.feed_checksum

             AND child_stop.parent_station =
                    parent_stop.stop_id

            WHERE
                scores.service_date = %s

                AND scores.route_id = %s

                AND scores.trip_id = %s

            ORDER BY
                scores.stop_sequence;
            """,
            (
                selected_date,
                selected_route,
                selected_trip,
            ),
        )


        if not stop_df.empty:

            stop_df = stop_df.rename(
                columns={
                    "stop_sequence":
                        "Stop Sequence",

                    "stop_id":
                        "Stop ID",

                    "station_name":
                        "Station",

                    "arrival_deviation_seconds":
                        "Arrival Deviation (sec)",

                    "arrival_uncertainty_seconds":
                        "Uncertainty (sec)",

                    "baseline_type":
                        "Baseline Type",

                    "baseline_count":
                        "Baseline Count",

                    "anomaly_score":
                        "Anomaly Score",

                    "anomaly_status":
                        "Anomaly Status",
                }
            )


            stop_df = convert_numeric(
                stop_df,
                [
                    "Stop Sequence",
                    "Arrival Deviation (sec)",
                    "Uncertainty (sec)",
                    "Baseline Count",
                    "Anomaly Score",
                ],
            )


            stop_df[
                "Anomaly Status"
            ] = (
                stop_df[
                    "Anomaly Status"
                ]
                .apply(
                    readable_status
                )
            )


            stop_df[
                "Baseline Type"
            ] = (
                stop_df[
                    "Baseline Type"
                ]
                .apply(
                    readable_status
                )
            )


            # =================================================
            # Stop chart
            # =================================================

            st.subheader(
                "Arrival Deviation by Station"
            )


            st.caption(
                "Positive values indicate arrival later than "
                "schedule; negative values indicate arrival "
                "earlier than schedule."
            )


            station_order = (
                stop_df
                .sort_values(
                    "Stop Sequence"
                )[
                    "Station"
                ]
                .tolist()
            )


            base_chart = (
                alt.Chart(
                    stop_df
                )

                .encode(

                    x=alt.X(
                        "Station:N",

                        title="Station",

                        sort=station_order,

                        axis=alt.Axis(
                            labelAngle=-35
                        ),
                    ),

                    y=alt.Y(
                        "Arrival Deviation (sec):Q",
                        title=(
                            "Arrival Deviation (seconds)"
                        ),
                    ),
                )
            )


            stop_line = (
                base_chart
                .mark_line()
            )


            stop_points = (
                base_chart

                .mark_circle(
                    size=85
                )

                .encode(

                    color=alt.Color(
                        "Anomaly Status:N",
                        title="Status",
                    ),

                    tooltip=[

                        alt.Tooltip(
                            "Station:N",
                            title="Station",
                        ),

                        alt.Tooltip(
                            "Stop Sequence:Q",
                            title="Sequence",
                        ),

                        alt.Tooltip(
                            "Stop ID:N",
                            title="Stop ID",
                        ),

                        alt.Tooltip(
                            "Arrival Deviation (sec):Q",
                            title="Arrival Deviation",
                            format=".1f",
                        ),

                        alt.Tooltip(
                            "Uncertainty (sec):Q",
                            title="Uncertainty",
                            format=".1f",
                        ),

                        alt.Tooltip(
                            "Anomaly Score:Q",
                            title="Anomaly Score",
                            format=".2f",
                        ),

                        alt.Tooltip(
                            "Anomaly Status:N",
                            title="Status",
                        ),

                        alt.Tooltip(
                            "Baseline Type:N",
                            title="Baseline",
                        ),

                        alt.Tooltip(
                            "Baseline Count:Q",
                            title=(
                                "Historical Observations"
                            ),
                        ),
                    ],
                )
            )


            zero_rule = (
                alt.Chart(
                    pd.DataFrame(
                        {
                            "zero": [0]
                        }
                    )
                )

                .mark_rule(
                    strokeDash=[
                        5,
                        5,
                    ]
                )

                .encode(
                    y="zero:Q"
                )
            )


            stop_chart = (
                stop_line
                + stop_points
                + zero_rule
            ).properties(
                height=380
            )


            st.altair_chart(
                stop_chart,
                use_container_width=True,
            )


            # =================================================
            # Stop detail table
            # =================================================

            st.subheader(
                "Stop Details"
            )


            display_stop_df = (
                stop_df[
                    [
                        "Stop Sequence",
                        "Station",
                        "Stop ID",
                        "Arrival Deviation (sec)",
                        "Uncertainty (sec)",
                        "Baseline Type",
                        "Baseline Count",
                        "Anomaly Score",
                        "Anomaly Status",
                    ]
                ]
            )


            st.dataframe(
                display_stop_df,
                use_container_width=True,
                hide_index=True,

                column_config={

                    "Arrival Deviation (sec)":
                        st.column_config.NumberColumn(
                            format="%.1f"
                        ),

                    "Uncertainty (sec)":
                        st.column_config.NumberColumn(
                            format="%.1f"
                        ),

                    "Anomaly Score":
                        st.column_config.NumberColumn(
                            format="%.2f"
                        ),
                },
            )


        else:

            st.info(
                "No scored stop observations are "
                "available for this trip."
            )


    else:

        st.info(
            "No trips match the selected classification."
        )


else:

    st.info(
        "No trip anomaly data is available "
        "for this route and service date."
    )