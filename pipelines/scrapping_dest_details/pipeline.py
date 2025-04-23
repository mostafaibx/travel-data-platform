import datetime
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

import pandas as pd

from .bigquery_loader import BigQueryLoader
from .config import BQ_TABLE_PATH, DATASET_ID, PROJECT_ID, TABLE_ID, TRAVEL_DESTINATIONS
from .fetcher import get_destination_info
from .gcs_storage import upload_processed_wiki_data, upload_raw_wiki_data

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def fetch_destinations_data() -> List[Dict[str, Any]]:
    """
    Fetch information about all travel destinations from Wikipedia

    Returns:
        List of processed destination data
    """
    destination_data = []
    failed_destinations = []
    raw_data_uploads = []

    logger.info(f"Starting to fetch data for {len(TRAVEL_DESTINATIONS)} destinations")

    # Set up thread pool for parallel processing
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Submit scraping tasks
        future_to_destination = {
            executor.submit(get_destination_info, destination): destination
            for destination in TRAVEL_DESTINATIONS
        }

        # Process results as they complete
        for future in as_completed(future_to_destination):
            destination = future_to_destination[future]

            try:
                info, raw_data = future.result()

                if info is None:
                    failed_destinations.append(destination)
                    logger.warning(f"Failed to get information for {destination}")
                    continue

                # Add timestamp
                info["ingestion_timestamp"] = datetime.datetime.now()

                # Add to our list of processed data
                destination_data.append(info)
                logger.info(f"Successfully processed data for {destination}")

                # Upload raw data to GCS
                if raw_data:
                    raw_data_upload_success = upload_raw_wiki_data(
                        destination, raw_data
                    )
                    raw_data_uploads.append((destination, raw_data_upload_success))

            except Exception as e:
                logger.error(f"Error processing {destination}: {e}")
                failed_destinations.append(destination)

    # Log summary
    logger.info(f"Successfully processed {len(destination_data)} destinations")
    if failed_destinations:
        logger.warning(
            f"Failed to process {len(failed_destinations)} destinations: {', '.join(failed_destinations)}"
        )

    return destination_data


def prepare_for_bigquery(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Prepare destination data for BigQuery by normalizing and flattening the structure

    Args:
        data: List of destination data dictionaries

    Returns:
        DataFrame with destination data including attractions as JSON array
    """
    if not data:
        logger.warning("No data to prepare for BigQuery")
        return pd.DataFrame()

    # Create a copy to avoid modifying the original data
    processed_data = []

    for item in data:
        destination_name = item.get("destination_name", "")

        # Calculate area if available
        area_km2 = None
        if "area" in item and isinstance(item["area"], dict):
            area_km2 = item["area"].get("km2")

        # Extract region/state information
        region = item.get("region", "")

        # Create a flattened record for destination with attractions as JSON array
        record = {
            "destination_name": destination_name,
            "description": item.get("description", ""),
            "country": item.get("country", ""),
            "latitude": item.get("coordinates", {}).get("latitude"),
            "longitude": item.get("coordinates", {}).get("longitude"),
            "population_count": item.get("population", {}).get("count"),
            "population_year": item.get("population", {}).get("year"),
            "timezone": item.get("timezone", ""),
            "languages": json.dumps(item.get("languages", [])),  # Store as JSON string
            "climate": item.get("climate", ""),
            "image_url": item.get("image_url", ""),
            "sections": json.dumps(item.get("sections", [])),  # Store as JSON string
            "area_km2": area_km2,
            "region": region,
            "attractions_count": len(item.get("attractions", [])),
            "attractions": json.dumps(
                item.get("attractions", [])
            ),  # Store attractions as JSON array
            "ingestion_timestamp": item.get(
                "ingestion_timestamp", datetime.datetime.now()
            ),
        }
        processed_data.append(record)

    # Convert to DataFrame
    df_destinations = pd.DataFrame(processed_data)

    # Log the count
    logger.info(f"Prepared {len(df_destinations)} destinations for BigQuery")

    return df_destinations


def run_pipeline() -> bool:
    """
    Main pipeline function to fetch Wikipedia data and upload to BigQuery and GCS

    Returns:
        Boolean indicating success or failure
    """
    logger.info("Starting Wikipedia scraping pipeline")

    try:
        # Fetch data from Wikipedia
        destination_data = fetch_destinations_data()
        logger.info(f"Fetched data for {len(destination_data)} destinations")

        # Upload raw data to GCS
        gcs_result = upload_processed_wiki_data(destination_data)
        if gcs_result:
            logger.info("Successfully uploaded processed data to GCS")
        else:
            logger.warning("Failed to upload processed data to GCS")

        # Prepare data for BigQuery
        df_destinations = prepare_for_bigquery(destination_data)

        # Upload to BigQuery using the modular loader
        loader = BigQueryLoader(PROJECT_ID, DATASET_ID, TABLE_ID)
        success = loader.upload_with_merge(df_destinations)

        if success:
            logger.info("Successfully uploaded all data to BigQuery")
        else:
            logger.warning("Failed to upload data to BigQuery")

        logger.info("Wikipedia scraping pipeline completed successfully")
        return True
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return False


if __name__ == "__main__":
    run_pipeline()
