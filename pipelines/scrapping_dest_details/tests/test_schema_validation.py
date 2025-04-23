"""
Tests for schema validation of the scrapping_dest_details pipeline outputs.
"""

import pytest
import pandas as pd
import json
from datetime import datetime
from great_expectations.dataset import PandasDataset
from ..pipeline import prepare_for_bigquery


def test_destination_schema_validation(sample_destination_data):
    """Test that the destination data conforms to our expected schema"""
    # Validate the raw destination data structure
    for destination in sample_destination_data:
        # Check required fields
        assert "destination_name" in destination
        assert "description" in destination
        assert "coordinates" in destination
        assert "country" in destination
        assert "population" in destination
        assert "attractions" in destination
        assert "ingestion_timestamp" in destination

        # Check data types
        assert isinstance(destination["destination_name"], str)
        assert isinstance(destination["description"], str)
        assert isinstance(destination["coordinates"], dict)
        assert isinstance(destination["country"], str)
        assert isinstance(destination["population"], dict)
        assert isinstance(destination["attractions"], list)
        assert isinstance(destination["ingestion_timestamp"], datetime)

        # Check nested structures
        assert "latitude" in destination["coordinates"]
        assert "longitude" in destination["coordinates"]
        assert "count" in destination["population"]

        # Check attractions structure
        for attraction in destination["attractions"]:
            assert "name" in attraction
            assert "description" in attraction
            assert "type" in attraction
            assert isinstance(attraction["name"], str)
            assert isinstance(attraction["description"], str)
            assert isinstance(attraction["type"], str)


def test_bigquery_schema_validation(sample_processed_df):
    """Test that the BigQuery DataFrame conforms to our expected schema"""
    # Convert to Great Expectations dataset for validation
    ge_df = PandasDataset(sample_processed_df)

    # Check column presence
    expected_columns = [
        "destination_name",
        "description",
        "country",
        "latitude",
        "longitude",
        "population_count",
        "population_year",
        "timezone",
        "languages",
        "climate",
        "image_url",
        "sections",
        "area_km2",
        "region",
        "attractions_count",
        "attractions",
        "ingestion_timestamp",
    ]
    for column in expected_columns:
        assert column in ge_df.columns

    # Check data types
    ge_df.expect_column_values_to_be_of_type("destination_name", "str")
    ge_df.expect_column_values_to_be_of_type("description", "str")
    ge_df.expect_column_values_to_be_of_type("country", "str")
    ge_df.expect_column_values_to_be_of_type("languages", "str")  # JSON string
    ge_df.expect_column_values_to_be_of_type("attractions", "str")  # JSON string

    # Numeric columns
    ge_df.expect_column_values_to_be_in_type_list(
        "latitude", ["float", "int", "float64", "int64"]
    )
    ge_df.expect_column_values_to_be_in_type_list(
        "longitude", ["float", "int", "float64", "int64"]
    )
    ge_df.expect_column_values_to_be_in_type_list(
        "population_count", ["float", "int", "float64", "int64"]
    )
    ge_df.expect_column_values_to_be_in_type_list(
        "attractions_count", ["float", "int", "float64", "int64"]
    )

    # Check value constraints
    ge_df.expect_column_values_to_not_be_null("destination_name")
    ge_df.expect_column_values_to_not_be_null("country")
    ge_df.expect_column_values_to_be_between("latitude", -90, 90)
    ge_df.expect_column_values_to_be_between("longitude", -180, 180)

    # Validate that JSON strings are parseable
    languages = json.loads(sample_processed_df.iloc[0]["languages"])
    assert isinstance(languages, list)

    attractions = json.loads(sample_processed_df.iloc[0]["attractions"])
    assert isinstance(attractions, list)

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
        assert dest_row["latitude"] == dest["coordinates"]["latitude"]
        assert dest_row["longitude"] == dest["coordinates"]["longitude"]
        assert dest_row["population_count"] == dest["population"].get("count")

        # Check attractions count
        assert dest_row["attractions_count"] == len(dest["attractions"])

        # Check serialized content
        attractions = json.loads(dest_row["attractions"])
        assert len(attractions) == len(dest["attractions"])

        for j, attr in enumerate(dest["attractions"]):
            assert attractions[j]["name"] == attr["name"]


def test_empty_data_handling():
    """Test handling of empty data"""
    # Verify that empty data doesn't cause errors
    empty_df = prepare_for_bigquery([])
    assert isinstance(empty_df, pd.DataFrame)
    assert len(empty_df) == 0
