"""BigQuery loader for the Weather API pipeline.

Appends transformed weather forecast data from a pandas DataFrame to
the configured BigQuery table with append semantics.
"""
import logging
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from .config import WeatherConfig

logger = logging.getLogger(__name__)


class BigQueryLoader:
    """Loads transformed weather data into BigQuery."""

    def __init__(self, config: WeatherConfig):
        self.config = config
        self.client = bigquery.Client(project=config.project_id)
        self.table_fqn = f"{config.project_id}.{config.dataset_id}.{config.table_id}"
        logger.info(
            "Initialized BigQuery loader",
            extra={"table": self.table_fqn},
        )

    def load_dataframe(self, data: pd.DataFrame) -> bool:
        """Load DataFrame into BigQuery table with append semantics.

        Returns True on success, False on failure.
        """
        try:
            if data is None or data.empty:
                logger.warning("No data to load into BigQuery", extra={"table": self.table_fqn})
                return True

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=True,
            )

            load_job = self.client.load_table_from_dataframe(
                data,
                destination=self.table_fqn,
                job_config=job_config,
            )
            load_job.result()  # Wait for completion

            table = self.client.get_table(self.table_fqn)
            logger.info(
                "Loaded data into BigQuery",
                extra={
                    "table": self.table_fqn,
                    "row_count": table.num_rows,
                },
            )
            return True
        except (ValueError, GoogleAPIError) as e:
            logger.exception(
                "Failed to load data into BigQuery",
                extra={"error": str(e), "table": self.table_fqn},
            )
            return False


