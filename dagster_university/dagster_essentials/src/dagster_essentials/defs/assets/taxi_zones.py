import dagster as dg
import duckdb
import os
from dagster_essentials.defs.assets import constants
from dagster._utils.backoff import backoff


@dg.asset(deps=["taxi_zones_file"])
def taxi_zones() -> None:
    """Load data to the DuckDB database."""

    query = f"""
        create or replace table taxi_zones as (
            select
                LocationID as zone_id,
                zone,
                borough,
                the_geom as geometry
            from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """

    connection = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE", "data/staging/data.duckdb")
        },
        max_retries=3
    )

    connection.execute(query=query)
