"""
Tests for the BigQuery loader module in the scrapping_dest_details pipeline.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from ..bigquery_loader import BigQueryLoader


class TestBigQueryLoader:
    """Tests for the BigQueryLoader class"""

    def test_init(self, monkeypatch):
        """Test initialization of the BigQueryLoader with mocked client"""
        # Mock the bigquery client
        mock_client = MagicMock()
        monkeypatch.setattr("google.cloud.bigquery.Client", lambda project: mock_client)
        
        # Initialize the loader
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")

        # Verify initialization
        assert loader.project_id == "test-project"
        assert loader.dataset_id == "test-dataset"
        assert loader.table_id == "test-table"
        assert loader.table_path == "test-project.test-dataset.test-table"
        assert loader.client == mock_client

    def test_ensure_dataset_exists_existing(self):
        """Test ensuring dataset exists when it already exists"""
        # Create mock client
        mock_client = MagicMock()
        
        # Create loader with mock client
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        loader.client = mock_client
        
        # Call the method
        loader._ensure_dataset_exists()
        
        # Verify client was called correctly
        mock_client.get_dataset.assert_called_once_with("test-dataset")
        mock_client.create_dataset.assert_not_called()

    def test_ensure_dataset_exists_create(self):
        """Test ensuring dataset exists when it needs to be created"""
        # Create mock client that raises NotFound for get_dataset
        mock_client = MagicMock()
        mock_client.get_dataset.side_effect = Exception("Dataset not found")
        
        # Create loader with mock client
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        loader.client = mock_client
        
        # Call the method
        loader._ensure_dataset_exists()
        
        # Verify client methods were called correctly
        mock_client.get_dataset.assert_called_once_with("test-dataset")
        mock_client.create_dataset.assert_called_once()

    def test_get_schema(self):
        """Test getting the schema for the destination table"""
        # Create loader
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        
        # Get schema
        schema = loader.get_schema()
        
        # Verify schema fields
        assert isinstance(schema, list)
        assert all(isinstance(field, bigquery.SchemaField) for field in schema)
        assert len(schema) > 0
        
        # Check required fields
        field_names = [field.name for field in schema]
        assert "destination_name" in field_names
        assert "description" in field_names
        assert "country" in field_names
        assert "ingestion_timestamp" in field_names

    def test_ensure_table_exists(self):
        """Test ensuring table exists"""
        # Create mock client
        mock_client = MagicMock()
        mock_dataset = MagicMock()
        mock_client.dataset.return_value = mock_dataset
        
        # Create loader with mock client
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        loader.client = mock_client
        
        # Mock get_schema
        with patch.object(loader, 'get_schema') as mock_get_schema:
            mock_get_schema.return_value = [
                bigquery.SchemaField("destination_name", "STRING"),
                bigquery.SchemaField("description", "STRING")
            ]
            
            # Call the method
            loader._ensure_table_exists()
            
            # Verify
            mock_client.dataset.assert_called_once_with("test-dataset")
            mock_dataset.table.assert_called_once_with("test-table")
            mock_client.create_table.assert_called_once()

    def test_create_temp_table(self):
        """Test creating a temporary table and loading data into it"""
        # Create mock client
        mock_client = MagicMock()
        mock_dataset = MagicMock()
        mock_client.dataset.return_value = mock_dataset
        mock_temp_table = MagicMock()
        mock_dataset.table.return_value = mock_temp_table
        
        # Mock the load job
        mock_load_job = MagicMock()
        mock_client.load_table_from_dataframe.return_value = mock_load_job
        
        # Create loader with mock client
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        loader.client = mock_client
        
        # Mock get_schema
        with patch.object(loader, 'get_schema') as mock_get_schema:
            mock_get_schema.return_value = [
                bigquery.SchemaField("destination_name", "STRING"),
                bigquery.SchemaField("description", "STRING")
            ]
            
            # Create test DataFrame
            test_df = pd.DataFrame({
                "destination_name": ["Paris"],
                "description": ["City of Lights"]
            })
            
            # Call the method
            temp_table_id = loader._create_temp_table(test_df)
            
            # Verify
            assert temp_table_id == "test-table_temp"
            mock_client.delete_table.assert_called_once_with(mock_temp_table)
            mock_client.create_table.assert_called_once()
            mock_client.load_table_from_dataframe.assert_called_once()
            mock_load_job.result.assert_called_once()

    def test_build_merge_query(self):
        """Test building a MERGE query"""
        # Create loader
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        
        # Mock get_schema
        with patch.object(loader, 'get_schema') as mock_get_schema:
            mock_get_schema.return_value = [
                bigquery.SchemaField("destination_name", "STRING"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("country", "STRING")
            ]
            
            # Call the method
            merge_query = loader._build_merge_query("temp_table")
            
            # Verify
            assert "MERGE" in merge_query
            assert "test-project.test-dataset.test-table" in merge_query
            assert "test-project.test-dataset.temp_table" in merge_query
            assert "target.destination_name = source.destination_name" in merge_query
            assert "description" in merge_query
            assert "country" in merge_query

    def test_execute_merge(self):
        """Test executing a MERGE operation"""
        # Create mock client
        mock_client = MagicMock()
        mock_dataset = MagicMock()
        mock_client.dataset.return_value = mock_dataset
        mock_table = MagicMock()
        mock_dataset.table.return_value = mock_table
        
        # Mock the query job
        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job
        
        # Mock the destination table
        mock_dest_table = MagicMock()
        mock_dest_table.num_rows = 10
        mock_client.get_table.return_value = mock_dest_table
        
        # Create loader with mock client
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        loader.client = mock_client
        
        # Mock build_merge_query
        with patch.object(loader, '_build_merge_query') as mock_build_query:
            mock_build_query.return_value = "MERGE QUERY"
            
            # Call the method
            num_rows = loader._execute_merge("temp_table")
            
            # Verify
            assert num_rows == 10
            mock_build_query.assert_called_once_with("temp_table")
            mock_client.query.assert_called_once_with("MERGE QUERY")
            mock_query_job.result.assert_called_once()
            mock_client.delete_table.assert_called_once()

    def test_upload_with_merge_success(self):
        """Test successful upload with MERGE operation"""
        # Create loader
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        
        # Mock required methods
        with patch.object(loader, '_ensure_dataset_exists') as mock_ensure_dataset, \
             patch.object(loader, '_ensure_table_exists') as mock_ensure_table, \
             patch.object(loader, '_create_temp_table') as mock_create_temp, \
             patch.object(loader, '_execute_merge') as mock_execute_merge:
            
            mock_create_temp.return_value = "temp_table_id"
            mock_execute_merge.return_value = 10
            
            # Create test DataFrame
            test_df = pd.DataFrame({
                "destination_name": ["Paris"],
                "description": ["City of Lights"]
            })
            
            # Call the method
            result = loader.upload_with_merge(test_df)
            
            # Verify
            assert result is True
            mock_ensure_dataset.assert_called_once()
            mock_ensure_table.assert_called_once()
            mock_create_temp.assert_called_once_with(test_df)
            mock_execute_merge.assert_called_once_with("temp_table_id")

    def test_upload_with_merge_empty_df(self):
        """Test upload with MERGE operation with empty DataFrame"""
        # Create loader
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        
        # Mock required methods
        with patch.object(loader, '_ensure_dataset_exists') as mock_ensure_dataset, \
             patch.object(loader, '_ensure_table_exists') as mock_ensure_table, \
             patch.object(loader, '_create_temp_table') as mock_create_temp, \
             patch.object(loader, '_execute_merge') as mock_execute_merge:
            
            # Create empty DataFrame
            empty_df = pd.DataFrame()
            
            # Call the method
            result = loader.upload_with_merge(empty_df)
            
            # Verify
            assert result is False
            mock_ensure_dataset.assert_called_once()
            mock_ensure_table.assert_called_once()
            mock_create_temp.assert_not_called()
            mock_execute_merge.assert_not_called()

    def test_upload_with_merge_exception(self):
        """Test handling exception during upload with MERGE operation"""
        # Create loader
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        
        # Mock required methods with exception
        with patch.object(loader, '_ensure_dataset_exists') as mock_ensure_dataset:
            mock_ensure_dataset.side_effect = Exception("Test error")
            
            # Create test DataFrame
            test_df = pd.DataFrame({
                "destination_name": ["Paris"],
                "description": ["City of Lights"]
            })
            
            # Call the method
            result = loader.upload_with_merge(test_df)
            
            # Verify
            assert result is False
            mock_ensure_dataset.assert_called_once()

    def test_append_data(self):
        """Test appending data to a BigQuery table"""
        # Create mock client
        mock_client = MagicMock()
        
        # Mock the load job
        mock_load_job = MagicMock()
        mock_client.load_table_from_dataframe.return_value = mock_load_job
        
        # Create loader with mock client
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        loader.client = mock_client
        
        # Create test DataFrame
        test_df = pd.DataFrame({
            "destination_name": ["Paris"],
            "description": ["City of Lights"]
        })
        
        # Mock required methods
        with patch.object(loader, '_ensure_dataset_exists') as mock_ensure_dataset, \
             patch.object(loader, '_ensure_table_exists') as mock_ensure_table:
            
            # Call the method
            result = loader.append_data(test_df)
            
            # Verify
            assert result is True
            mock_ensure_dataset.assert_called_once()
            mock_ensure_table.assert_called_once()
            mock_client.load_table_from_dataframe.assert_called_once()
            mock_load_job.result.assert_called_once()
