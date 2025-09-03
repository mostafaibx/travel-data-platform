"""Google Cloud Storage operations for the Weather API pipeline.

Handles storing both raw API responses and transformed data as gzipped JSON.
"""
import gzip
import logging
from datetime import datetime
import pandas as pd
from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError

from .config import WeatherConfig

logger = logging.getLogger(__name__)

class GCSStorage:
    """Handles storage operations to Google Cloud Storage."""
    
    def __init__(self, config: WeatherConfig):
        """Initialize GCS storage handler.
        
        Args:
            config: Configuration containing GCS settings
        """
        self.config = config
        self.client = storage.Client()
        self.bucket = self.client.bucket(config.gcs_bucket)
        
        logger.info("Initialized GCS storage handler", 
                   extra={"bucket": config.gcs_bucket})

    def _compress_data(self, data: str) -> bytes:
        """Compress string data using gzip.
        
        Args:
            data: String data to compress
            
        Returns:
            bytes: Compressed data
        """
        return gzip.compress(data.encode('utf-8'))

    def store_raw_responses(self, raw_payloads: list[dict]) -> bool:
        """Store raw API responses in GCS as gzipped JSON.

        The payload is expected to be a list of dicts with keys like
        {city, country, latitude, longitude, response}.
        """
        try:
            if not raw_payloads:
                logger.warning("No raw payloads to store in GCS")
                return True

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            blob_name = f"weather_raw/{timestamp}_raw_responses.json.gz"
            blob = self.bucket.blob(blob_name)

            json_str = pd.Series(raw_payloads).to_json(orient='values')
            compressed = self._compress_data(json_str)

            blob.content_encoding = 'gzip'
            blob.upload_from_string(compressed, content_type='application/json')

            logger.info(
                "Stored raw API responses in GCS",
                extra={"blob": blob_name, "size_bytes": len(compressed)},
            )
            return True
        except (ValueError, TypeError, GoogleAPIError) as e:
            logger.exception("Failed to store raw responses in GCS", extra={"error": str(e)})
            return False
