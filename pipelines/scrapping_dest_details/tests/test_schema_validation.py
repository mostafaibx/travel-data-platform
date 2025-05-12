"""
Tests for schema validation of the scrapping_dest_details pipeline outputs.
"""

import json
from datetime import datetime

import pandas as pd

# Update Great Expectations import to use current version structure
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.execution_engine.pandas_execution_engine import (
    PandasExecutionEngine,
)

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


def test_bigquery_schema_validation(sample_processed_df):
    """Test that the BigQuery DataFrame conforms to our expected schema"""
    # Simplify test to use direct pandas assertions without Great Expectations
    # Check column presence
    expected_columns = [
        "destination_name",
        "description",
        "country",
        "continent",
        "climate",
        "main_attractions",
        "ingestion_timestamp",
    ]
    for column in expected_columns:
        assert column in sample_processed_df.columns

    # Check data types
    assert sample_processed_df["destination_name"].dtype == object  # string
    assert sample_processed_df["description"].dtype == object  # string
    assert sample_processed_df["country"].dtype == object  # string

    # Check value constraints
    assert not sample_processed_df["destination_name"].isnull().any()
    assert not sample_processed_df["country"].isnull().any()

    # Check formatting
    for _, row in sample_processed_df.iterrows():
        assert row["destination_name"].strip() == row["destination_name"]
        assert not row["description"].startswith(" ")
        assert not row["country"].startswith(" ")


def test_data_transformation_consistency(sample_destination_data):
    """Test that the data transformation from raw to BigQuery format is consistent"""
    # Transform raw data to BigQuery format
    bq_df = prepare_for_bigquery(sample_destination_data)

    # Check that all destinations are present
    assert len(bq_df) == len(sample_destination_data)

    # Check that key information is preserved
    for i, dest in enumerate(sample_destination_data):
        # Find corresponding row in DataFrame
        dest_row = bq_df[bq_df["destination_name"] == dest["destination_name"]].iloc[0]

        # Check that key fields match
        assert dest_row["description"] == dest["description"]
        assert dest_row["country"] == dest["country"]
        assert dest_row["climate"] == dest["climate"]

        # Check attractions
        if isinstance(dest_row["main_attractions"], list):
            assert len(dest_row["main_attractions"]) == len(dest["main_attractions"])
        else:
            # If stored as string (JSON), convert back
            attractions = (
                json.loads(dest_row["main_attractions"])
                if isinstance(dest_row["main_attractions"], str)
                else dest_row["main_attractions"]
            )
            assert len(attractions) == len(dest["main_attractions"])


def test_empty_data_handling():
    """Test handling of empty data"""
    # Verify that empty data doesn't cause errors
    empty_df = prepare_for_bigquery([])
    assert isinstance(empty_df, pd.DataFrame)
    assert len(empty_df) == 0
