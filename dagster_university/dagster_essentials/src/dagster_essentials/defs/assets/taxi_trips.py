import dagster as dg
from dagster._utils.backoff import backoff
import os
import duckdb


@dg.asset(deps=["trips"])
def taxi_trips() -> None:
    """Load data into the DuckDB database."""

    query = """
        create or replace table trips as (
          select
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_code_id,
            payment_type as payment_type,
            tpep_dropoff_datetime as dropoff_datetime,
            tpep_pickup_datetime as pickup_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount
          from 'data/raw/taxi_trips_2023-03.parquet'
        );
    """

    connection = backoff(
        duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        max_retries=3,
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE", "data/staging/data.duckdb")
        }
    )

    connection.execute(query)