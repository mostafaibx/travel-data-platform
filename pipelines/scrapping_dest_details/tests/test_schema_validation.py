"""
Tests for schema validation of the scrapping_dest_details pipeline outputs.
"""

import json
from datetime import datetime

import pandas as pd
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.execution_engine.pandas_execution_engine import PandasExecutionEngine

from ..pipeline import prepare_for_bigquery


def test_destination_schema_validation(sample_destination_data):
    """Test that the destination data conforms to our expected schema"""
    # Validate the raw destination data structure
    for destination in sample_destination_data:
        # Check required fields
        assert "destination_name" in destination
        assert "country" in destination
        assert "description" in destination
        assert "climate" in destination
        assert "main_attractions" in destination
        assert "ingestion_timestamp" in destination

        # Check data types
        assert isinstance(destination["destination_name"], str)
        assert isinstance(destination["description"], str)
        assert isinstance(destination["country"], str)
        assert isinstance(destination["main_attractions"], list)
        assert isinstance(destination["ingestion_timestamp"], str)

        # Check attractions structure
        for attraction in destination["main_attractions"]:
            assert isinstance(attraction, str)


def test_data_transformation_consistency(sample_destination_data):
    """Test that the data transformation from raw to BigQuery format is consistent"""
    # Transform data to pandas DataFrame for BigQuery
    bq_df = prepare_for_bigquery(sample_destination_data)
    
    # Verify DataFrame structure
    assert isinstance(bq_df, pd.DataFrame)
    assert len(bq_df) == len(sample_destination_data)
    
    # Verify required columns are present
    required_columns = [
        "destination_name", 
        "description", 
        "country", 
        "climate", 
        "main_attractions", 
        "ingestion_timestamp"
    ]
    for column in required_columns:
        assert column in bq_df.columns
    
    # Verify data consistency between input and output
    for i, dest in enumerate(sample_destination_data):
        # Find corresponding row in DataFrame
        dest_row = bq_df[bq_df["destination_name"] == dest["destination_name"]].iloc[0]
        
        # Check core fields match
        assert dest_row["destination_name"] == dest["destination_name"]
        assert dest_row["description"] == dest["description"]
        assert dest_row["country"] == dest["country"]
        assert dest_row["climate"] == dest["climate"]
        
        # Verify main_attractions are handled correctly
        if isinstance(dest_row["main_attractions"], str):
            # If stored as JSON string, parse and compare
            attractions_json = json.loads(dest_row["main_attractions"])
            assert set(attractions_json) == set(dest["main_attractions"])
        else:
            # If stored as Python list, compare directly
            assert set(dest_row["main_attractions"]) == set(dest["main_attractions"])


def test_empty_data_handling():
    """Test handling of empty data"""
    # Verify that empty data doesn't cause errors
    empty_df = prepare_for_bigquery([])
    assert isinstance(empty_df, pd.DataFrame)
    assert len(empty_df) == 0


def test_schema_structure(sample_processed_df):
    """Test the structure of the data schema"""
    # Verify column data types are appropriate
    assert sample_processed_df["destination_name"].dtype == object  # string
    assert sample_processed_df["description"].dtype == object  # string
    assert sample_processed_df["country"].dtype == object  # string
    
    # Check for null values in key fields
    assert not sample_processed_df["destination_name"].isnull().any()
    assert not sample_processed_df["country"].isnull().any()
    
    # Verify data formatting
    for _, row in sample_processed_df.iterrows():
        # Check string fields are properly trimmed
        assert row["destination_name"].strip() == row["destination_name"]
        assert row["country"].strip() == row["country"]
        
        # Verify main_attractions is either a list or a valid JSON string
        if isinstance(row["main_attractions"], str):
            # Should be parseable as JSON
            attractions = json.loads(row["main_attractions"])
            assert isinstance(attractions, list)
        elif isinstance(row["main_attractions"], list):
            # Should contain only strings
            assert all(isinstance(item, str) for item in row["main_attractions"])


def test_datetime_handling(sample_destination_data):
    """Test handling of datetime values"""
    # Transform data to pandas DataFrame
    bq_df = prepare_for_bigquery(sample_destination_data)
    
    # Check ingestion_timestamp handling
    assert "ingestion_timestamp" in bq_df.columns
    
    for i, dest in enumerate(sample_destination_data):
        # Get corresponding row from DataFrame
        dest_row = bq_df[bq_df["destination_name"] == dest["destination_name"]].iloc[0]
        
        # Verify timestamp conversion
        if isinstance(dest_row["ingestion_timestamp"], str):
            # If stored as string, check format
            assert dest["ingestion_timestamp"] == dest_row["ingestion_timestamp"]
        elif pd.api.types.is_datetime64_any_dtype(dest_row["ingestion_timestamp"]):
            # If stored as datetime, convert to string for comparison
            df_timestamp = dest_row["ingestion_timestamp"].strftime("%Y-%m-%dT%H:%M:%S")
            # Remove milliseconds if present in the original
            orig_timestamp = dest["ingestion_timestamp"].split(".")[0]
            assert df_timestamp == orig_timestamp
