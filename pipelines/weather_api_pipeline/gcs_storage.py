import datetime
import json
import logging
from typing import Any, Dict, List

from google.cloud import storage

from .config import BUCKET_NAME, PROJECT_ID, WEATHER_RAW_PATH

logger = logging.getLogger(__name__)

# Custom JSON encoder to handle datetime objects


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)


def upload_raw_data_to_gcs(
    data: List[Dict[str, Any]], raw_responses: Dict[str, Dict[str, Any]]
) -> bool:
    """
    Upload raw weather data as JSON to Google Cloud Storage.

    Args:
        data: Processed weather data for BigQuery (used for metadata)
        raw_responses: Raw responses from the Weather API

    Returns:
        bool: True if upload was successful, False otherwise
    """
    try:
        # Initialize the storage client
        storage_client = storage.Client(project=PROJECT_ID)

        # Get bucket, create if doesn't exist
        try:
            bucket = storage_client.get_bucket(BUCKET_NAME)
        except Exception:
            bucket = storage_client.create_bucket(BUCKET_NAME)
            logger.info(f"Created bucket {BUCKET_NAME}")

        # Create a timestamp-based folder structure
        today = datetime.datetime.now()
        year_month = today.strftime("%Y/%m")
        day = today.strftime("%d")

        # Path format: weather_data/raw/YYYY/MM/DD/weather_data_YYYYMMDD.json
        file_timestamp = today.strftime("%Y%m%d")
        object_name = (
            f"{WEATHER_RAW_PATH}/{year_month}/{day}/weather_data_{file_timestamp}.json"
        )

        # Create JSON payload with raw data and metadata
        payload = {
            "metadata": {
                "timestamp": today.isoformat(),
                "source": "OpenWeather API",
                "locations_count": len(raw_responses),
                "records_count": len(data),
                "data_days": 2,  # We're fetching 2 days of data
            },
            "raw_data": raw_responses,
        }

        # Convert to string using custom encoder for datetime objects
        json_data = json.dumps(payload, indent=2, cls=DateTimeEncoder)

        # Upload to GCS
        blob = bucket.blob(object_name)
        blob.upload_from_string(json_data, content_type="application/json")

        logger.info(
            f"Successfully uploaded raw weather data to gs://{BUCKET_NAME}/{object_name}"
        )
        return True

    except Exception as e:
        logger.error(f"Error uploading raw data to GCS: {e}")
        return False
