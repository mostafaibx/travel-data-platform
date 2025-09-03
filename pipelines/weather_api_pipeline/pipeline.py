"""Weather pipeline orchestration and data flow control.

Coordinates fetching, normalization, raw storage, and BigQuery loading
of weather forecast data.
"""
import logging
from typing import Optional
import pandas as pd

from pipelines.common.logging_conf import configure_logging
from .config import WeatherConfig
from .fetcher import WeatherFetcher
from .gcs_storage import GCSStorage
from .bigquery_loader import BigQueryLoader

logger = logging.getLogger(__name__)

class WeatherPipeline:
    """Pipeline for fetching and storing weather data."""
    
    def __init__(self, config: WeatherConfig, run_id: Optional[str] = None):
        """Initialize the pipeline with configuration.
        
        Args:
            config: Pipeline configuration
            run_id: Optional run ID for correlation. If None, generates a new UUID.
        """
        self.config = config
        configure_logging("weather_pipeline", run_id)
        
        logger.info("Initializing Weather Pipeline", 
                   extra={"config": config.dict(exclude={"api_key"}), "masked": True})
        
        self.fetcher = WeatherFetcher(config)
        self.storage = GCSStorage(config)
        self.bq_loader = BigQueryLoader(config)

    def _normalize_forecast_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Normalize forecast data with proper types and UTC timestamps."""
        # Convert forecast_date to datetime64[ns]
        data['forecast_date'] = pd.to_datetime(data['forecast_date']).dt.tz_localize('UTC')
        
        # Ensure all timestamp fields are in UTC
        timestamp_columns = [col for col in data.columns if 'time' in col.lower()]
        for col in timestamp_columns:
            if pd.api.types.is_datetime64_any_dtype(data[col]):
                data[col] = data[col].dt.tz_localize('UTC')
        
        return data

    def run(self) -> bool:
        """Execute the weather data pipeline.
        
        Returns:
            bool: True if pipeline executed successfully, False otherwise
        """
        try:
            logger.info("Starting weather data pipeline execution")
            
            # Fetch weather data and raw payloads
            fetch_result = self.fetcher.fetch_weather_data()
            if not fetch_result:
                logger.error("Failed to fetch weather data")
                return False
            raw_data, raw_payloads = fetch_result
                
            # Normalize data types and timestamps
            processed_data = self._normalize_forecast_data(raw_data)
            
            # Store raw payloads for lineage
            if not self.storage.store_raw_responses(raw_payloads):
                logger.error("Failed to store raw API responses in GCS")
                return False

            # Load processed data to BigQuery
            if not self.bq_loader.load_dataframe(processed_data):
                logger.error("Failed to load weather data into BigQuery")
                return False
            
            logger.info("Weather pipeline completed successfully")
            return True
            
        except (ValueError, KeyError) as e:
            logger.exception("Pipeline execution failed", 
                           extra={"error": str(e), "masked": True})
            return False
