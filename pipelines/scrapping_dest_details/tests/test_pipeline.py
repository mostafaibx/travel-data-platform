"""
Tests for the pipeline module in the scrapping_dest_details pipeline.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
import datetime
import json
from ..pipeline import fetch_destinations_data, prepare_for_bigquery, run_pipeline


@patch("pipelines.scrapping_dest_details.pipeline.get_destination_info")
@patch("pipelines.scrapping_dest_details.pipeline.upload_raw_wiki_data")
def test_fetch_destinations_data(
    mock_upload_raw, mock_get_dest_info, sample_destination_data, mock_raw_data
):
    """Test fetching destination data from Wikipedia"""
    # Mock the destination list
    test_destinations = ["Paris", "Rome", "London"]

    with patch(
        "pipelines.scrapping_dest_details.pipeline.TRAVEL_DESTINATIONS",
        test_destinations,
    ):
        # Configure mock to return sample data for each destination
        mock_get_dest_info.side_effect = [
            (sample_destination_data[0], mock_raw_data),
            (sample_destination_data[1], mock_raw_data),
            (None, None),  # Simulate failure for London
        ]

        # Set raw data upload to succeed
        mock_upload_raw.return_value = True

        # Call the function
        result = fetch_destinations_data()

        # Verify function calls
        assert mock_get_dest_info.call_count == 3
        assert mock_upload_raw.call_count == 2  # Only successful destinations

        # Verify results
        assert len(result) == 2  # Two successful destinations
        assert result[0]["destination_name"] == "Paris"
        assert result[1]["destination_name"] == "Rome"

        # Verify timestamps were added
        assert "ingestion_timestamp" in result[0]
        assert isinstance(result[0]["ingestion_timestamp"], datetime.datetime)


def test_prepare_for_bigquery(sample_destination_data):
    """Test preparing destination data for BigQuery"""
    df = prepare_for_bigquery(sample_destination_data)

    # Check DataFrame structure
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2

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
    for col in expected_columns:
        assert col in df.columns

    # Check values
    assert df.iloc[0]["destination_name"] == "Paris"
    assert df.iloc[1]["destination_name"] == "Rome"

    # Check serialized JSON fields
    assert isinstance(df.iloc[0]["languages"], str)
    languages = json.loads(df.iloc[0]["languages"])
    assert "French" in languages

    assert isinstance(df.iloc[0]["attractions"], str)
    attractions = json.loads(df.iloc[0]["attractions"])
    assert attractions[0]["name"] == "Eiffel Tower"

    # Check attraction counting
    assert df.iloc[0]["attractions_count"] == 2
    assert df.iloc[1]["attractions_count"] == 2


def test_prepare_for_bigquery_empty():
    """Test preparing empty data for BigQuery"""
    df = prepare_for_bigquery([])
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


@patch("pipelines.scrapping_dest_details.pipeline.fetch_destinations_data")
@patch("pipelines.scrapping_dest_details.pipeline.upload_processed_wiki_data")
@patch("pipelines.scrapping_dest_details.pipeline.prepare_for_bigquery")
@patch("pipelines.scrapping_dest_details.pipeline.BigQueryLoader")
def test_run_pipeline_success(
    mock_bq_loader_class,
    mock_prepare,
    mock_upload_processed,
    mock_fetch,
    sample_destination_data,
    sample_processed_df,
    mock_bq_loader,
):
    """Test running the pipeline successfully"""
    # Configure mocks
    mock_fetch.return_value = sample_destination_data
    mock_upload_processed.return_value = True
    mock_prepare.return_value = sample_processed_df
    mock_bq_loader_class.return_value = mock_bq_loader

    # Run the pipeline
    result = run_pipeline()

    # Verify success
    assert result is True

    # Verify function calls
    mock_fetch.assert_called_once()
    mock_upload_processed.assert_called_once_with(sample_destination_data)
    mock_prepare.assert_called_once_with(sample_destination_data)
    mock_bq_loader.upload_with_merge.assert_called_once_with(sample_processed_df)


@patch("pipelines.scrapping_dest_details.pipeline.fetch_destinations_data")
def test_run_pipeline_failure(mock_fetch):
    """Test pipeline failure handling"""
    # Configure mock to raise an exception
    mock_fetch.side_effect = Exception("Network error")

    # Run the pipeline
    result = run_pipeline()

    # Verify failure
    assert result is False
    mock_fetch.assert_called_once()


@patch("pipelines.scrapping_dest_details.pipeline.fetch_destinations_data")
@patch("pipelines.scrapping_dest_details.pipeline.upload_processed_wiki_data")
@patch("pipelines.scrapping_dest_details.pipeline.prepare_for_bigquery")
@patch("pipelines.scrapping_dest_details.pipeline.BigQueryLoader")
def test_run_pipeline_bq_failure(
    mock_bq_loader_class,
    mock_prepare,
    mock_upload_processed,
    mock_fetch,
    sample_destination_data,
    sample_processed_df,
):
    """Test handling BigQuery upload failure"""
    # Configure mocks
    mock_fetch.return_value = sample_destination_data
    mock_upload_processed.return_value = True
    mock_prepare.return_value = sample_processed_df

    # Configure BigQuery failure
    mock_bq_loader = MagicMock()
    mock_bq_loader.upload_with_merge.return_value = False
    mock_bq_loader_class.return_value = mock_bq_loader

    # Run the pipeline
    result = run_pipeline()

    # Pipeline should still return True even with BigQuery failure
    # as the logs will capture the warning
    assert result is True
