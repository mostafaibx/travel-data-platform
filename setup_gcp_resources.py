#!/usr/bin/env python
"""
Setup script for GCP resources needed by the travel data platform
This script creates the necessary GCS buckets and validates BigQuery access
"""

import argparse
import logging
import os

from dotenv import load_dotenv
from google.cloud import bigquery, storage

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get GCP configuration from environment
PROJECT_ID = os.getenv("BQ_PROJECT_ID", "travler-data-platform")
GCS_RAW_BUCKET = os.getenv("GCS_BUCKET_NAME", "travel-data-raw")
GCS_WEATHER_BUCKET = os.getenv("GCS_WEATHER_BUCKET_NAME", "travel-data-platform-raw")
BQ_DATASET_ID = os.getenv("BQ_STAGING_DATASET_ID", "staging")


def setup_gcs_buckets():
    """Create GCS buckets if they don't exist"""
    try:
        storage_client = storage.Client(project=PROJECT_ID)

        # Create the raw data bucket
        buckets_to_create = [GCS_RAW_BUCKET, GCS_WEATHER_BUCKET]

        for bucket_name in buckets_to_create:
            try:
                storage_client.get_bucket(bucket_name)
                logger.info(f"Bucket {bucket_name} already exists")
            except Exception:
                # Create bucket with standard settings
                storage_client.create_bucket(bucket_name, location="us-central1")
                logger.info(f"Created bucket {bucket_name}")

        return True
    except Exception as e:
        logger.error(f"Error setting up GCS buckets: {e}")
        return False


def validate_bigquery_access():
    """Validate BigQuery access and create dataset if needed"""
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)

        # Try to get the dataset
        dataset_id = f"{PROJECT_ID}.{BQ_DATASET_ID}"

        try:
            bq_client.get_dataset(dataset_id)
            logger.info(f"Dataset {dataset_id} already exists")
        except Exception:
            # Create the dataset
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            bq_client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created dataset {dataset_id}")

        # Validate billing is enabled by trying to run a simple query
        query = "SELECT 1"
        query_job = bq_client.query(query)
        list(query_job.result())  # Run the query to validate billing

        logger.info("BigQuery connection and billing validated successfully")
        return True
    except Exception as e:
        logger.error(f"Error validating BigQuery access: {e}")
        if "billing" in str(e).lower():
            logger.error(
                "This appears to be a billing-related issue. Please make sure billing is enabled for your project."
            )
            logger.error(
                "Visit https://console.cloud.google.com/billing/linkedaccount?project="
                + PROJECT_ID
            )
        return False


def main():
    """Main function to set up all GCP resources"""
    parser = argparse.ArgumentParser(
        description="Set up GCP resources for the travel data platform"
    )
    parser.add_argument(
        "--skip-bigquery", action="store_true", help="Skip BigQuery validation"
    )
    parser.add_argument(
        "--skip-gcs", action="store_true", help="Skip GCS bucket creation"
    )
    args = parser.parse_args()

    logger.info(f"Setting up GCP resources for project {PROJECT_ID}")

    success = True

    if not args.skip_gcs:
        if setup_gcs_buckets():
            logger.info("GCS buckets setup completed successfully")
        else:
            success = False
            logger.error("GCS buckets setup failed")

    if not args.skip_bigquery:
        if validate_bigquery_access():
            logger.info("BigQuery access validated successfully")
        else:
            success = False
            logger.error("BigQuery access validation failed")

    if success:
        logger.info("All GCP resources set up successfully")
        return 0
    else:
        logger.error("GCP resource setup failed")
        return 1


if __name__ == "__main__":
    exit(main())
