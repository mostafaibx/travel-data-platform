import datetime
import logging

# import os
from typing import Any, Dict, List, Tuple

import pandas as pd
from google.cloud import bigquery

from .config import BQ_TABLE_PATH, DATASET_ID, PROJECT_ID, TABLE_ID
from .fetcher import get_weather_data

# from .gcs_storage import upload_raw_data_to_gcs

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# List of popular travel destinations with their coordinates
# Format: (city, country, latitude, longitude)
TRAVEL_DESTINATIONS = [
    ("New York", "US", 40.7128, -74.0060),
    ("London", "UK", 51.5074, -0.1278),
    ("Paris", "FR", 48.8566, 2.3522),
    ("Tokyo", "JP", 35.6762, 139.6503),
    ("Sydney", "AU", -33.8688, 151.2093),
    ("Rome", "IT", 41.9028, 12.4964),
    ("Bangkok", "TH", 13.7563, 100.5018),
    ("Dubai", "AE", 25.2048, 55.2708),
    ("Barcelona", "ES", 41.3851, 2.1734),
    ("Cancun", "MX", 21.1619, -86.8515),
    ("Bali", "ID", -8.3405, 115.0920),
    ("Cape Town", "ZA", -33.9249, 18.4241),
    ("Miami", "US", 25.7617, -80.1918),
    ("Cairo", "EG", 30.0444, 31.2357),
    ("Singapore", "SG", 1.3521, 103.8198),
]


def fetch_forecast_data() -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Fetch weather forecast data for all travel destinations
    Returns:
        - processed data ready for BigQuery
        - raw responses from the API for GCS storage
    """
    today = datetime.datetime.now().date()
    data_rows = []
    raw_responses = {}

    for city, country, lat, lon in TRAVEL_DESTINATIONS:
        location_key = f"{city}_{country}"
        try:
            # Get the full forecast including daily data for upcoming days
            forecast = get_weather_data(
                lat=lat, lon=lon, exclude=["minutely", "hourly", "alerts"]
            )

            # Store the raw API response
            raw_responses[location_key] = forecast

            if "daily" not in forecast:
                logger.warning(
                    f"No daily forecast data available for {city}, {country}"
                )
                continue

            # Process forecast data for the next 2 days (today and tomorrow)
            for i in range(min(2, len(forecast["daily"]))):
                day_data = forecast["daily"][i]
                forecast_date = today + datetime.timedelta(days=i)
                forecast_timestamp = datetime.datetime.fromtimestamp(day_data["dt"])
                sunrise_time = datetime.datetime.fromtimestamp(day_data["sunrise"])
                sunset_time = datetime.datetime.fromtimestamp(day_data["sunset"])

                # Extract relevant weather information for travel analysis
                weather_row = {
                    "city": city,
                    "country": country,
                    "latitude": lat,
                    "longitude": lon,
                    "forecast_date": forecast_date,  # Store as Python date object
                    "forecast_timestamp": forecast_timestamp,  # Store as Python datetime object
                    "max_temp": day_data["temp"]["max"],
                    "min_temp": day_data["temp"]["min"],
                    "day_temp": day_data["temp"]["day"],
                    "night_temp": day_data["temp"]["night"],
                    "feels_like_day": day_data["feels_like"]["day"],
                    "feels_like_night": day_data["feels_like"]["night"],
                    "humidity": day_data["humidity"],
                    "wind_speed": day_data["wind_speed"],
                    "weather_main": day_data["weather"][0]["main"],
                    "weather_description": day_data["weather"][0]["description"],
                    "weather_icon": day_data["weather"][0]["icon"],
                    "precipitation_probability": day_data.get("pop", 0),
                    "rain": day_data.get("rain", 0),
                    "uvi": day_data.get("uvi", 0),
                    "clouds": day_data.get("clouds", 0),
                    "sunrise": sunrise_time,  # Store as Python datetime object
                    "sunset": sunset_time,  # Store as Python datetime object
                    "ingestion_timestamp": datetime.datetime.now(),  # Store as Python datetime object
                }
                data_rows.append(weather_row)

            logger.info(f"Successfully fetched forecast data for {city}, {country}")
        except Exception as e:
            logger.error(f"Error fetching data for {city}, {country}: {e}")

    return data_rows, raw_responses


def upload_to_bigquery(data: List[Dict[str, Any]]) -> None:
    """
    Upload processed weather data to BigQuery
    """
    if not data:
        logger.warning("No data to upload to BigQuery")
        return

    try:
        # Convert to DataFrame for easier handling
        df = pd.DataFrame(data)

        # Configure BigQuery client
        client = bigquery.Client(project=PROJECT_ID)

        # Check if dataset exists, create if not
        try:
            client.get_dataset(DATASET_ID)
        except Exception:
            dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
            dataset.location = "US"
            client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created dataset {DATASET_ID}")

        # Define schema (matches our data structure)
        schema = [
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
            bigquery.SchemaField("forecast_date", "DATE"),
            bigquery.SchemaField("forecast_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("max_temp", "FLOAT"),
            bigquery.SchemaField("min_temp", "FLOAT"),
            bigquery.SchemaField("day_temp", "FLOAT"),
            bigquery.SchemaField("night_temp", "FLOAT"),
            bigquery.SchemaField("feels_like_day", "FLOAT"),
            bigquery.SchemaField("feels_like_night", "FLOAT"),
            bigquery.SchemaField("humidity", "INTEGER"),
            bigquery.SchemaField("wind_speed", "FLOAT"),
            bigquery.SchemaField("weather_main", "STRING"),
            bigquery.SchemaField("weather_description", "STRING"),
            bigquery.SchemaField("weather_icon", "STRING"),
            bigquery.SchemaField("precipitation_probability", "FLOAT"),
            bigquery.SchemaField("rain", "FLOAT"),
            bigquery.SchemaField("uvi", "FLOAT"),
            bigquery.SchemaField("clouds", "INTEGER"),
            bigquery.SchemaField("sunrise", "TIMESTAMP"),
            bigquery.SchemaField("sunset", "TIMESTAMP"),
            bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP"),
        ]

        # Configure table with schema
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        table = bigquery.Table(table_ref, schema=schema)

        # Create table if it doesn't exist
        client.create_table(table, exists_ok=True)

        # Load data into table
        job_config = bigquery.LoadJobConfig(
            schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for job to complete

        logger.info(f"Successfully loaded {len(data)} rows into {BQ_TABLE_PATH}")

    except Exception as e:
        logger.error(f"Error uploading data to BigQuery: {e}")
        raise


def run_pipeline():
    """
    Main pipeline function to fetch weather data and upload to BigQuery and GCS
    """
    logger.info("Starting weather data pipeline")

    try:
        # Fetch data from Weather API (returns both processed and raw data)
        weather_data, raw_responses = fetch_forecast_data()
        logger.info(f"Fetched forecast data for {len(weather_data)} location-days")

        # Upload processed data to BigQuery
        upload_to_bigquery(weather_data)

        # Upload raw data to GCS - temporarily disabled due to billing issue
        # To enable, set up billing on GCP project or use a different bucket
        # gcs_result = upload_raw_data_to_gcs(weather_data, raw_responses)
        # if gcs_result:
        #     logger.info("Successfully uploaded raw data to GCS")
        # else:
        #     logger.warning("Failed to upload raw data to GCS")

        logger.info("Pipeline completed successfully")
        return True
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return False


if __name__ == "__main__":
    run_pipeline()
