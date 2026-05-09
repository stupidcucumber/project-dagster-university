import dagster as dg
import requests
from dagster_essentials.defs.assets import constants


@dg.asset
def taxi_zones_file() -> None:
    """
    The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    data = requests.get("https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv")

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as f:
        f.write(data.content)