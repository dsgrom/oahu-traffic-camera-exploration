# Honolulu Traffic Incident data found here: https://data.honolulu.gov/Public-Safety/Traffic-Incidents/ykb6-n5th/about_data

from pathlib import Path

from loguru import logger
# from tqdm import tqdm
import typer

from oahu_traffic_camera_exploration.config import DATABASE_URL, APP_TOKEN, RAW_DATA_DIR

import pandas as pd
from sodapy import Socrata
from datetime import datetime

app = typer.Typer()


def get_results_filename():
    # Get the current local date and time as a datetime object
    current_datetime = datetime.now()
    timestamp_str = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")  # Format: 2025-01-04_05-33-00

    return f"{timestamp_str}_pull.csv"

@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    # 1. Grab credentials
    database_url: str = DATABASE_URL,
    app_token: str = APP_TOKEN,
    # 2. Create the complete filename
    output_path: Path = RAW_DATA_DIR / get_results_filename()
    # ----------------------------------------------
):
    # ---- ACCESS DATABASE API AND PULL DOWN DATA AS CSV ----
    logger.info("Processing dataset...")

    # Unauthenticated client only works with public data sets.
    # Unauthenticated would be 'None' in place of application token,
    # and no username or password:
    client = Socrata(
        database_url,
        app_token
    )

    # First 2000 results, returned as JSON from API / converted to Python list of dictionaries by sodapy.
    results = client.get("ykb6-n5th")  # limit syntax: limit=2000 if needed
    results_df = pd.DataFrame.from_records(results)
    results_df.to_csv(output_path)

    logger.success("Processing dataset complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
