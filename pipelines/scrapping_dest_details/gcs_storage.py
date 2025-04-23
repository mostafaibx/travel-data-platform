"""
Module for storing data in Google Cloud Storage.
"""

# Remove unused imports
import datetime
import json
from typing import Any, Dict, List

from google.cloud import storage

# Configuration
from .config import BUCKET_NAME


def _get_storage_client():
    """
    Get a Google Cloud Storage client

    Returns:
        storage.Client: The GCS client
    """
    return storage.Client()


def _get_bucket():
    """
    Get the GCS bucket for storing data

    Returns:
        storage.Bucket: The GCS bucket
    """
    client = _get_storage_client()
    return client.bucket(BUCKET_NAME)


def upload_raw_wiki_data(destination: str, data: Dict[str, Any]) -> bool:
    """
    Upload raw Wikipedia data for a destination to GCS

    Args:
        destination: The destination name
        data: The raw data to upload

    Returns:
        bool: True if upload was successful, False otherwise
    """
    try:
        # Get the bucket
        bucket = _get_bucket()

        # Create a timestamp-based path for the data
        now = datetime.datetime.now()
        date_path = now.strftime("%Y/%m/%d")
        timestamp = now.strftime("%Y%m%d_%H%M%S")

        # Clean destination name for use in filename
        clean_dest = destination.replace(" ", "_")

        # Create the blob path
        blob_path = f"raw/wiki/{date_path}/{clean_dest}_{timestamp}.json"

        # Get a blob object
        blob = bucket.blob(blob_path)

        # Upload the data as JSON
        blob.upload_from_string(
            json.dumps(data, ensure_ascii=False, default=str),
            content_type="application/json",
        )

        return True

    except Exception as e:
        print(f"Error uploading raw wiki data for {destination}: {e}")
        return False


def upload_processed_wiki_data(data: List[Dict[str, Any]]) -> bool:
    """
    Upload processed Wikipedia data to GCS

    Args:
        data: The processed data to upload

    Returns:
        bool: True if upload was successful, False otherwise
    """
    try:
        # Get the bucket
        bucket = _get_bucket()

        # Create a timestamp-based filename
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_path = f"processed/wiki/destinations_{timestamp}.json"

        # Get a blob object
        blob = bucket.blob(blob_path)

        # Upload the data as JSON, handling datetime objects
        blob.upload_from_string(
            json.dumps(data, ensure_ascii=False, default=str),
            content_type="application/json",
        )

        return True

    except Exception as e:
        print(f"Error uploading processed wiki data: {e}")
        return False
