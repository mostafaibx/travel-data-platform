import os
import glob
import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas as pd
from datetime import datetime
from typing import Optional

from . import config
from pipelines.common.gcp_auth import get_bigquery_client

# Set up logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('bigquery_ingestion')

class BigQueryIngestion:
    """Handles ingestion of travel data CSV files into BigQuery."""
    
    def __init__(self, key_path: Optional[str] = None, project_id: Optional[str] = None):
        """
        Initialize the BigQuery ingestion client.
        
        Args:
            key_path: Optional path to a service account key file
            project_id: Optional project ID to use (defaults to config.PROJECT_ID or from credentials)
        """
        # Use provided project_id or fall back to config
        self.project_id = project_id or config.PROJECT_ID
        
        # Get authenticated client using the common authentication module
        self.client = get_bigquery_client(
            key_path=key_path,
            project_id=self.project_id,
            location="US"  # Default location
        )
        
        # Set up BigQuery resources
        self._ensure_dataset_exists()
        self._ensure_table_exists()
        logger.info("BigQuery client initialized")
    
    def _ensure_dataset_exists(self):
        """Create the dataset if it doesn't exist."""
        dataset_ref = self.client.dataset(config.DATASET_ID)
        try:
            self.client.get_dataset(dataset_ref)
            logger.debug(f"Dataset {config.DATASET_ID} already exists")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Set the dataset location
            dataset = self.client.create_dataset(dataset)
            logger.info(f"Dataset {config.DATASET_ID} created")
    
    def _ensure_table_exists(self):
        """Create the table if it doesn't exist."""
        table_ref = self.client.dataset(config.DATASET_ID).table(config.TABLE_ID)
        try:
            self.client.get_table(table_ref)
            logger.debug(f"Table {config.TABLE_ID} already exists")
        except NotFound:
            # Define schema based on the travel data structure
            schema = [
                bigquery.SchemaField("Trip_ID", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("Destination", "STRING"),
                bigquery.SchemaField("Start_date", "DATE"),
                bigquery.SchemaField("End_date", "DATE"),
                bigquery.SchemaField("Duration_days", "INTEGER"),
                bigquery.SchemaField("Traveler_name", "STRING"),
                bigquery.SchemaField("Traveler_age", "INTEGER"),
                bigquery.SchemaField("Traveler_gender", "STRING"),
                bigquery.SchemaField("Traveler_nationality", "STRING"),
                bigquery.SchemaField("Accommodation_type", "STRING"),
                bigquery.SchemaField("Accommodation_cost", "FLOAT"),
                bigquery.SchemaField("Transportation_type", "STRING"),
                bigquery.SchemaField("Transportation_cost", "FLOAT"),
                bigquery.SchemaField("Ingestion_timestamp", "TIMESTAMP"),
                bigquery.SchemaField("Source_file", "STRING"),
            ]
            
            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table)
            logger.info(f"Table {config.TABLE_ID} created")
    
    def _transform_data(self, df, source_file):
        """Transform the data to match BigQuery schema."""
        df_transformed = df.copy()
        
        # Handle dates
        df_transformed['Start_date'] = pd.to_datetime(df_transformed['Start date']).dt.date
        df_transformed['End_date'] = pd.to_datetime(df_transformed['End date']).dt.date
        
        # Rename columns to match schema (remove spaces)
        df_transformed = df_transformed.rename(columns={
            'Trip ID': 'Trip_ID',
            'Duration (days)': 'Duration_days',
            'Traveler name': 'Traveler_name',
            'Traveler age': 'Traveler_age',
            'Traveler gender': 'Traveler_gender',
            'Traveler nationality': 'Traveler_nationality',
            'Accommodation type': 'Accommodation_type',
            'Accommodation cost': 'Accommodation_cost',
            'Transportation type': 'Transportation_type',
            'Transportation cost': 'Transportation_cost'
        })
        
        # Convert cost columns to float
        for col in ['Accommodation_cost', 'Transportation_cost']:
            if col in df_transformed.columns:
                df_transformed[col] = df_transformed[col].apply(
                    lambda x: float(str(x).replace('$', '').replace(',', '').strip()) 
                    if pd.notnull(x) and str(x).strip() else None
                )
        
        # Add metadata columns
        df_transformed['Ingestion_timestamp'] = datetime.now()
        df_transformed['Source_file'] = os.path.basename(source_file)
        
        # Drop original date columns and other unnecessary columns
        df_transformed = df_transformed.drop(['Start date', 'End date'], axis=1, errors='ignore')
        
        return df_transformed
    
    def ingest_file(self, file_path):
        """Ingest a single CSV file into BigQuery."""
        try:
            # Read the file
            df = pd.read_csv(file_path)
            
            # Transform data
            df_transformed = self._transform_data(df, file_path)
            
            # Define job config
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            
            # Upload to BigQuery
            table_ref = self.client.dataset(config.DATASET_ID).table(config.TABLE_ID)
            job = self.client.load_table_from_dataframe(
                df_transformed, table_ref, job_config=job_config
            )
            job.result()  # Wait for the job to complete
            
            # Mark file as processed by moving it to processed directory
            processed_path = config.PROCESSED_DATA_DIR / os.path.basename(file_path)
            os.rename(file_path, processed_path)
            
            logger.info(f"Successfully ingested {file_path} to BigQuery and moved to {processed_path}")
            return True
        except Exception as e:
            logger.error(f"Error ingesting file {file_path}: {str(e)}")
            return False
    
    def ingest_all_pending_files(self):
        """Ingest all pending CSV files in the raw data directory."""
        raw_files = glob.glob(str(config.RAW_DATA_DIR / "*.csv"))
        
        if not raw_files:
            logger.info("No pending files to ingest")
            return 0
        
        success_count = 0
        for file_path in raw_files:
            if self.ingest_file(file_path):
                success_count += 1
        
        logger.info(f"Ingested {success_count} out of {len(raw_files)} files")
        return success_count


def ingest_files_to_bigquery(key_path: Optional[str] = None, project_id: Optional[str] = None):
    """
    Ingest all pending files to BigQuery.
    
    Args:
        key_path: Optional path to a service account key file
        project_id: Optional project ID to use
        
    Returns:
        Number of successfully ingested files
    """
    ingestion = BigQueryIngestion(key_path=key_path, project_id=project_id)
    return ingestion.ingest_all_pending_files()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Ingest travel data to BigQuery")
    parser.add_argument("--key-path", help="Path to service account key file")
    parser.add_argument("--project-id", help="Google Cloud project ID")
    
    args = parser.parse_args()
    
    ingest_files_to_bigquery(key_path=args.key_path, project_id=args.project_id) 