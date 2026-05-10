import geopandas as gpd
import duckdb
import os
import dagster as dg
import matplotlib.pyplot as plt
import pandas as pd
import datetime

from dagster_essentials.defs.assets import constants
from dagster._utils.backoff import backoff


@dg.asset(deps=["taxi_trips", "taxi_zones"])
def manhattan_stats() -> None:
    """Get number of trips per borough in Manhattan."""

    query = """
        select 
            taxi_zones.zone,
            taxi_zones.borough,
            taxi_zones.geometry,
            count(*) as num_trips
        from
            taxi_zones join trips on taxi_zones.zone_id = trips.pickup_zone_id
        where borough = 'Manhattan' and geometry is not null
        group by taxi_zones.zone, taxi_zones.borough, taxi_zones.geometry;
    """

    connection = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        max_retries=3,
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE", "data/staging/data.duckdb")
        }
    )

    data = connection.execute(query).fetch_df()
    data["geometry"] = gpd.GeoSeries.from_wkt(data["geometry"])
    data = gpd.GeoDataFrame(data)

    with open(constants.MANHATTAN_STATS_FILE_PATH, "w") as f:
        f.write(data.to_json(indent=4))


@dg.asset(deps=["manhattan_stats"])
def manhattan_map() -> None:
    """Visualize manhattan map of trips."""

    data = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    data.plot(column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black")
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim(-74.05, -73.90)  # Adjust longitude range
    ax.set_ylim(40.70, 40.82)  # Adjust latitude range
    
    # Save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)


@dg.asset(deps=["taxi_trips"])
def trips_by_week() -> None:
    """Create a CSV file of a trips by week."""

    query = """
        select
            DATE_TRUNC('week', pickup_datetime) + interval '6 day' as period, 
            COUNT(*) as num_trips,
            SUM(passenger_count) as passenger_count,
            SUM(total_amount) as total_amount,
            SUM(trip_distance) as trip_distance
        from trips
        group by DATE_TRUNC('week', pickup_datetime) + interval '6 day'
        having period >= '2023-03-05' and period <= '2023-04-01'
        order by DATE_TRUNC('day', MAX(pickup_datetime));
    """

    connection = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        max_retries=3,
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE", "data/staging/data.duckdb")
        }
    )

    data = connection.execute(query).fetch_df()
    data.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH)

    connection.close()


@dg.asset(deps=["taxi_trips"])
def trips_by_week_memory_optimised() -> None:
    """Optimized versio of 'trips_by_week'."""

    query = """
        select
			DATE_TRUNC('day', MAX(pickup_datetime)) as period,
            COUNT(*) as num_trips,
            SUM(passenger_count) as passenger_count,
            SUM(total_amount) as total_amount,
            SUM(trip_distance) as trip_distance
        from trips
        where pickup_datetime >= '{date}'::DATE - interval '1 week' + interval '1 day' and pickup_datetime < '{date}'::DATE + interval '1 day';
    """

    connection = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        max_retries=3,
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE", "data/staging/data.duckdb")
        }
    )

    dfs = []

    date = datetime.date.fromisoformat("2023-03-05") # This is Sunday

    while date <= datetime.date.fromisoformat("2023-04-01"):

        df = connection.execute(query.format(date=date.isoformat())).fetch_df()

        dfs.append(df)

        date += datetime.timedelta(weeks=1)

    data: pd.DataFrame = pd.concat(dfs, ignore_index=True)
    data.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH)

    connection.close()