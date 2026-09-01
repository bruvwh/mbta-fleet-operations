import csv
import zipfile
from pathlib import Path
from datetime import datetime

import psycopg


def get_connection():
    return psycopg.connect(
        host="localhost",
        port=5432,
        dbname="mbta",
        user="mbta",
        password="mbta",
    )


def parse_int(value):
    if value is None or value == "":
        return None

    return int(value)


def parse_float(value):
    if value is None or value == "":
        return None

    return float(value)

def parse_gtfs_date(value):
    if not value:
        return None

    return datetime.strptime(
        value,
        "%Y%m%d"
    ).date()

def load_calendar(
    cursor,
    gtfs_zip,
    feed_checksum
):

    count = 0

    with cursor.copy(
        """
        COPY gtfs_calendar (
            feed_checksum,
            service_id,
            monday,
            tuesday,
            wednesday,
            thursday,
            friday,
            saturday,
            sunday,
            start_date,
            end_date
        )
        FROM STDIN
        """
    ) as copy:

        for row in read_gtfs_file(
            gtfs_zip,
            "calendar.txt"
        ):

            copy.write_row(
                (
                    feed_checksum,
                    row["service_id"],
                    int(row["monday"]),
                    int(row["tuesday"]),
                    int(row["wednesday"]),
                    int(row["thursday"]),
                    int(row["friday"]),
                    int(row["saturday"]),
                    int(row["sunday"]),
                    parse_gtfs_date(
                        row["start_date"]
                    ),
                    parse_gtfs_date(
                        row["end_date"]
                    ),
                )
            )

            count += 1

    return count

def load_calendar_dates(
    cursor,
    gtfs_zip,
    feed_checksum
):

    count = 0

    with cursor.copy(
        """
        COPY gtfs_calendar_dates (
            feed_checksum,
            service_id,
            service_date,
            exception_type
        )
        FROM STDIN
        """
    ) as copy:

        for row in read_gtfs_file(
            gtfs_zip,
            "calendar_dates.txt"
        ):

            copy.write_row(
                (
                    feed_checksum,
                    row["service_id"],
                    parse_gtfs_date(
                        row["date"]
                    ),
                    int(
                        row["exception_type"]
                    ),
                )
            )

            count += 1

    return count


def gtfs_time_to_seconds(value):
    if not value:
        return None

    hours, minutes, seconds = map(
        int,
        value.split(":")
    )

    return (
        hours * 3600
        + minutes * 60
        + seconds
    )


def read_gtfs_file(gtfs_zip, filename):

    file = gtfs_zip.open(filename)

    text_file = (
        line.decode("utf-8-sig")
        for line in file
    )

    return csv.DictReader(text_file)


def get_unloaded_feed(cursor):

    cursor.execute(
        """
        SELECT
            feed_checksum,
            file_path
        FROM gtfs_feeds
        WHERE schedule_loaded_at IS NULL
        ORDER BY downloaded_at
        LIMIT 1
        """
    )

    return cursor.fetchone()


def load_routes(
    cursor,
    gtfs_zip,
    feed_checksum
):

    count = 0

    with cursor.copy(
        """
        COPY gtfs_routes (
            feed_checksum,
            route_id,
            agency_id,
            route_short_name,
            route_long_name,
            route_type,
            route_color,
            route_text_color
        )
        FROM STDIN
        """
    ) as copy:

        for row in read_gtfs_file(
            gtfs_zip,
            "routes.txt"
        ):

            copy.write_row(
                (
                    feed_checksum,
                    row["route_id"],
                    row.get("agency_id"),
                    row.get("route_short_name"),
                    row.get("route_long_name"),
                    parse_int(
                        row.get("route_type")
                    ),
                    row.get("route_color"),
                    row.get("route_text_color"),
                )
            )

            count += 1

    return count


def load_stops(
    cursor,
    gtfs_zip,
    feed_checksum
):

    count = 0

    with cursor.copy(
        """
        COPY gtfs_stops (
            feed_checksum,
            stop_id,
            stop_name,
            stop_lat,
            stop_lon,
            location_type,
            parent_station,
            platform_code,
            wheelchair_boarding
        )
        FROM STDIN
        """
    ) as copy:

        for row in read_gtfs_file(
            gtfs_zip,
            "stops.txt"
        ):

            copy.write_row(
                (
                    feed_checksum,
                    row["stop_id"],
                    row.get("stop_name"),
                    parse_float(
                        row.get("stop_lat")
                    ),
                    parse_float(
                        row.get("stop_lon")
                    ),
                    parse_int(
                        row.get("location_type")
                    ),
                    row.get("parent_station")
                    or None,
                    row.get("platform_code")
                    or None,
                    parse_int(
                        row.get(
                            "wheelchair_boarding"
                        )
                    ),
                )
            )

            count += 1

    return count


def load_trips(
    cursor,
    gtfs_zip,
    feed_checksum
):

    count = 0

    with cursor.copy(
        """
        COPY gtfs_trips (
            feed_checksum,
            trip_id,
            route_id,
            service_id,
            trip_headsign,
            direction_id,
            block_id,
            shape_id,
            wheelchair_accessible
        )
        FROM STDIN
        """
    ) as copy:

        for row in read_gtfs_file(
            gtfs_zip,
            "trips.txt"
        ):

            copy.write_row(
                (
                    feed_checksum,
                    row["trip_id"],
                    row["route_id"],
                    row["service_id"],
                    row.get("trip_headsign"),
                    parse_int(
                        row.get("direction_id")
                    ),
                    row.get("block_id")
                    or None,
                    row.get("shape_id")
                    or None,
                    parse_int(
                        row.get(
                            "wheelchair_accessible"
                        )
                    ),
                )
            )

            count += 1

    return count


def load_stop_times(
    cursor,
    gtfs_zip,
    feed_checksum
):

    count = 0

    with cursor.copy(
        """
        COPY gtfs_stop_times (
            feed_checksum,
            trip_id,
            stop_sequence,
            stop_id,
            arrival_time,
            departure_time,
            arrival_seconds,
            departure_seconds,
            pickup_type,
            drop_off_type,
            timepoint
        )
        FROM STDIN
        """
    ) as copy:

        for row in read_gtfs_file(
            gtfs_zip,
            "stop_times.txt"
        ):

            arrival = row.get(
                "arrival_time"
            )

            departure = row.get(
                "departure_time"
            )

            copy.write_row(
                (
                    feed_checksum,
                    row["trip_id"],
                    parse_int(
                        row["stop_sequence"]
                    ),
                    row["stop_id"],
                    arrival or None,
                    departure or None,
                    gtfs_time_to_seconds(
                        arrival
                    ),
                    gtfs_time_to_seconds(
                        departure
                    ),
                    parse_int(
                        row.get("pickup_type")
                    ),
                    parse_int(
                        row.get("drop_off_type")
                    ),
                    parse_int(
                        row.get("timepoint")
                    ),
                )
            )

            count += 1

    return count


def load_schedule():

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            feed = get_unloaded_feed(
                cursor
            )

            if feed is None:
                print(
                    "No new GTFS schedule "
                    "needs loading."
                )
                return

            feed_checksum = feed[0]
            filepath = Path(feed[1])

            print(
                f"Loading GTFS feed: "
                f"{feed_checksum[:12]}"
            )

            print(
                f"Source: {filepath}"
            )

            with zipfile.ZipFile(
                filepath
            ) as gtfs_zip:

                route_count = load_routes(
                    cursor,
                    gtfs_zip,
                    feed_checksum,
                )

                print(
                    f"Routes loaded: "
                    f"{route_count}"
                )

                stop_count = load_stops(
                    cursor,
                    gtfs_zip,
                    feed_checksum,
                )

                print(
                    f"Stops loaded: "
                    f"{stop_count}"
                )

                trip_count = load_trips(
                    cursor,
                    gtfs_zip,
                    feed_checksum,
                )

                print(
                    f"Trips loaded: "
                    f"{trip_count}"
                )

                stop_time_count = (
                    load_stop_times(
                        cursor,
                        gtfs_zip,
                        feed_checksum,
                    )
                )

                print(
                    f"Stop times loaded: "
                    f"{stop_time_count}"
                )

                calendar_count = load_calendar(
                    cursor,
                    gtfs_zip,
                    feed_checksum,
                )

                print(
                    f"Calendar rows loaded: "
                    f"{calendar_count}"
                )

                calendar_dates_count = load_calendar_dates(
                    cursor,
                    gtfs_zip,
                    feed_checksum,
                )

                print(
                    f"Calendar date rows loaded: "
                    f"{calendar_dates_count}"
                )

            cursor.execute(
                """
                UPDATE gtfs_feeds
                SET schedule_loaded_at = NOW()
                WHERE feed_checksum = %s
                """,
                (feed_checksum,),
            )

        connection.commit()

        print()
        print(
            "GTFS schedule successfully "
            "loaded."
        )

    except Exception:

        connection.rollback()

        print(
            "Load failed. Transaction "
            "rolled back."
        )

        raise

    finally:

        connection.close()


if __name__ == "__main__":
    load_schedule()