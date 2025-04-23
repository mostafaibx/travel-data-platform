import logging
import pandas as pd
from typing import List
from google.cloud import bigquery

logger = logging.getLogger(__name__)

class BigQueryLoader:
    """
    A class to handle BigQuery loading operations with efficient MERGE handling for updates
    """
    
    def __init__(self, project_id: str, dataset_id: str, table_id: str, location: str = "EU"):
        """
        Initialize the BigQuery loader
        
        Args:
            project_id: Google Cloud project ID
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            location: Dataset location (default: EU)
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.location = location
        self.client = bigquery.Client(project=project_id)
        self.table_path = f"{project_id}.{dataset_id}.{table_id}"
    
    def _ensure_dataset_exists(self) -> None:
        """
        Check if dataset exists and create it if not
        """
        try:
            self.client.get_dataset(self.dataset_id)
            logger.debug(f"Dataset {self.dataset_id} already exists")
        except Exception:
            dataset = bigquery.Dataset(f"{self.project_id}.{self.dataset_id}")
            dataset.location = self.location
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created dataset {self.dataset_id} in {self.location}")
    
    def get_schema(self) -> List[bigquery.SchemaField]:
        """
        Define and return schema for destinations table
        
        Returns:
            List of BigQuery SchemaField objects defining the table schema
        """
        return [
            bigquery.SchemaField("destination_name", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
            bigquery.SchemaField("population_count", "INTEGER"),
            bigquery.SchemaField("population_year", "INTEGER"),
            bigquery.SchemaField("timezone", "STRING"),
            bigquery.SchemaField("languages", "STRING"),  # JSON array as string
            bigquery.SchemaField("climate", "STRING"),
            bigquery.SchemaField("image_url", "STRING"),
            bigquery.SchemaField("sections", "STRING"),  # JSON array as string
            bigquery.SchemaField("area_km2", "FLOAT"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("attractions_count", "INTEGER"),
            bigquery.SchemaField("attractions", "STRING"),  # JSON array of attractions
            bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP")
        ]
    
    def _ensure_table_exists(self) -> None:
        """
        Create the table if it doesn't exist
        """
        schema = self.get_schema()
        table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
        table = bigquery.Table(table_ref, schema=schema)
        self.client.create_table(table, exists_ok=True)
        logger.debug(f"Ensured table {self.table_id} exists")
    
    def _create_temp_table(self, df: pd.DataFrame) -> str:
        """
        Create a temporary table and load data into it
        
        Args:
            df: DataFrame with data to load
        
        Returns:
            Temporary table ID
        """
        temp_table_id = f"{self.table_id}_temp"
        temp_table_ref = self.client.dataset(self.dataset_id).table(temp_table_id)
        
        # Create or recreate the temp table
        try:
            self.client.delete_table(temp_table_ref)
            logger.info(f"Deleted existing temp table {temp_table_id}")
        except Exception:
            logger.debug(f"No existing temp table {temp_table_id} to delete")
        
        # Create the temp table with the schema
        temp_table = bigquery.Table(temp_table_ref, schema=self.get_schema())
        self.client.create_table(temp_table)
        logger.info(f"Created temp table {temp_table_id}")
        
        # Load data into the temp table
        job_config = bigquery.LoadJobConfig(
            schema=self.get_schema(),
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        load_job = self.client.load_table_from_dataframe(
            df, 
            temp_table_ref, 
            job_config=job_config
        )
        load_job.result()  # Wait for the job to complete
        logger.info(f"Loaded {len(df)} rows into temp table {temp_table_id}")
        
        return temp_table_id
    
    def _build_merge_query(self, temp_table_id: str) -> str:
        """
        Build MERGE query for updating the main table
        
        Args:
            temp_table_id: ID of the temporary table with new data
        
        Returns:
            MERGE query string
        """
        # Collect all column names from schema excluding destination_name (the key)
        schema = self.get_schema()
        column_names = [field.name for field in schema if field.name != 'destination_name']
        
        # Build the WHEN MATCHED conditions
        match_conditions = " OR ".join([f"target.{col} != source.{col}" for col in column_names])
        
        # Build the update SET clause
        update_sets = ",\n                    ".join([f"{col} = source.{col}" for col in column_names])
        
        # Build the insert columns and values
        insert_columns = ", ".join(["destination_name"] + column_names)
        insert_values = ", ".join(["source.destination_name"] + [f"source.{col}" for col in column_names])
        
        # Construct full MERGE query
        merge_query = f"""
        MERGE `{self.project_id}.{self.dataset_id}.{self.table_id}` AS target
        USING `{self.project_id}.{self.dataset_id}.{temp_table_id}` AS source
        ON target.destination_name = source.destination_name
        
        WHEN MATCHED AND (
            {match_conditions}
        ) THEN
            UPDATE SET
                {update_sets}
        
        WHEN NOT MATCHED THEN
            INSERT (
                {insert_columns}
            )
            VALUES (
                {insert_values}
            )
        """
        return merge_query
    
    def _execute_merge(self, temp_table_id: str) -> int:
        """
        Execute the MERGE operation and clean up
        
        Args:
            temp_table_id: ID of the temporary table with new data
        
        Returns:
            Number of rows in the destination table after merge
        """
        merge_query = self._build_merge_query(temp_table_id)
        
        # Execute the MERGE
        merge_job = self.client.query(merge_query)
        merge_job.result()
        
        # Get stats about the operation
        temp_table_ref = self.client.dataset(self.dataset_id).table(temp_table_id)
        destination_table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
        destination_table = self.client.get_table(destination_table_ref)
        num_rows = destination_table.num_rows
        
        # Clean up the temp table
        self.client.delete_table(temp_table_ref)
        logger.info(f"Deleted temp table {temp_table_id}")
        
        return num_rows
    
    def upload_with_merge(self, df: pd.DataFrame) -> bool:
        """
        Upload data to BigQuery using a MERGE operation for efficient updates
        
        Args:
            df: DataFrame with destination data
        
        Returns:
            Boolean indicating success or failure
        """
        success = False
        
        try:
            # Setup BigQuery resources
            self._ensure_dataset_exists()
            self._ensure_table_exists()
            
            if not df.empty:
                # Create temp table and load data
                temp_table_id = self._create_temp_table(df)
                
                # Execute merge operation
                num_rows = self._execute_merge(temp_table_id)
                
                logger.info(f"MERGE operation completed. Total rows in destination table: {num_rows}")
                success = True
            else:
                logger.warning("No destination data to upload to BigQuery")
            
            return success
            
        except Exception as e:
            logger.error(f"Error uploading data to BigQuery: {e}")
            return success
    
    def append_data(self, df: pd.DataFrame) -> bool:
        """
        Simply append data to BigQuery table without performing merge/updates
        
        Args:
            df: DataFrame with destination data
        
        Returns:
            Boolean indicating success or failure
        """
        success = False
        
        try:
            # Setup BigQuery resources
            self._ensure_dataset_exists()
            self._ensure_table_exists()
            
            if not df.empty:
                # Configure job to append data
                job_config = bigquery.LoadJobConfig(
                    schema=self.get_schema(),
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                )
                
                # Load data into the table
                table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
                load_job = self.client.load_table_from_dataframe(
                    df, 
                    table_ref, 
                    job_config=job_config
                )
                load_job.result()  # Wait for job to complete
                
                logger.info(f"Successfully appended {len(df)} rows to {self.table_path}")
                success = True
            else:
                logger.warning("No destination data to upload to BigQuery")
            
            return success
            
        except Exception as e:
            logger.error(f"Error appending data to BigQuery: {e}")
            return success