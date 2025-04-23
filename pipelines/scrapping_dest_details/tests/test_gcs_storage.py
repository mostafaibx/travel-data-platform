"""
Tests for the GCS storage module in the scrapping_dest_details pipeline.
"""

import datetime
import json
from unittest.mock import MagicMock, patch

from ..gcs_storage import (
    _get_bucket,
    _get_storage_client,
    upload_processed_wiki_data,
    upload_raw_wiki_data,
)


@patch("pipelines.scrapping_dest_details.gcs_storage._get_bucket")
@patch("pipelines.scrapping_dest_details.gcs_storage.datetime")
def test_upload_raw_wiki_data(mock_datetime, mock_get_bucket, mock_raw_data):
    """Test uploading raw wiki data to GCS"""
    # Set up mocks
    mock_bucket = MagicMock()
    mock_get_bucket.return_value = mock_bucket
    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob

    # Mock datetime to get consistent timestamps
    mock_datetime.datetime.now.return_value = datetime.datetime(2023, 5, 15, 12, 0, 0)
    mock_datetime.datetime.side_effect = lambda *args, **kw: datetime.datetime(
        *args, **kw
    )

    # Call the function
    result = upload_raw_wiki_data("Paris", mock_raw_data)

    # Verify result
    assert result is True

    # Verify the correct blob was created
    expected_path = "raw/wiki/2023/05/15/Paris_20230515_120000.json"
    mock_bucket.blob.assert_called_once_with(expected_path)

    # Verify the blob was uploaded with the correct content
    mock_blob.upload_from_string.assert_called_once()
    call_args = mock_blob.upload_from_string.call_args[0]
    uploaded_json = call_args[0]
    uploaded_data = json.loads(uploaded_json)
    assert uploaded_data == mock_raw_data


@patch("pipelines.scrapping_dest_details.gcs_storage._get_bucket")
@patch("pipelines.scrapping_dest_details.gcs_storage.datetime")
def test_upload_raw_wiki_data_failure(mock_datetime, mock_get_bucket):
    """Test handling failure when uploading raw data"""
    # Set up mocks
    mock_bucket = MagicMock()
    mock_get_bucket.return_value = mock_bucket
    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_blob.upload_from_string.side_effect = Exception("Upload error")

    # Mock datetime
    mock_datetime.datetime.now.return_value = datetime.datetime(2023, 5, 15, 12, 0, 0)
    mock_datetime.datetime.side_effect = lambda *args, **kw: datetime.datetime(
        *args, **kw
    )

    # Call the function
    result = upload_raw_wiki_data("Paris", {"test": "data"})

    # Verify result
    assert result is False


@patch("pipelines.scrapping_dest_details.gcs_storage._get_bucket")
@patch("pipelines.scrapping_dest_details.gcs_storage.datetime")
def test_upload_processed_wiki_data(
    mock_datetime, mock_get_bucket, sample_destination_data
):
    """Test uploading processed wiki data to GCS"""
    # Set up mocks
    mock_bucket = MagicMock()
    mock_get_bucket.return_value = mock_bucket
    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob

    # Mock datetime to get consistent timestamps
    mock_datetime.datetime.now.return_value = datetime.datetime(2023, 5, 15, 12, 0, 0)
    mock_datetime.datetime.side_effect = lambda *args, **kw: datetime.datetime(
        *args, **kw
    )

    # Call the function
    result = upload_processed_wiki_data(sample_destination_data)

    # Verify result
    assert result is True

    # Verify the correct blob was created
    expected_path = "processed/wiki/destinations_20230515_120000.json"
    mock_bucket.blob.assert_called_once_with(expected_path)

    # Verify the blob was uploaded with the correct content
    mock_blob.upload_from_string.assert_called_once()

    # Ensure datetime objects were converted to strings for JSON serialization
    call_args = mock_blob.upload_from_string.call_args[0]
    uploaded_json = call_args[0]
    uploaded_data = json.loads(uploaded_json)

    # Check that data was serialized properly
    assert len(uploaded_data) == 2
    assert uploaded_data[0]["destination_name"] == "Paris"
    assert uploaded_data[1]["destination_name"] == "Rome"
    # Check that datetime was properly converted to string
    assert isinstance(uploaded_data[0]["ingestion_timestamp"], str)


@patch("pipelines.scrapping_dest_details.gcs_storage._get_bucket")
def test_upload_processed_wiki_data_failure(mock_get_bucket):
    """Test handling failure when uploading processed data"""
    # Set up mocks
    mock_bucket = MagicMock()
    mock_get_bucket.return_value = mock_bucket
    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_blob.upload_from_string.side_effect = Exception("Upload error")

    # Call the function
    result = upload_processed_wiki_data([{"test": "data"}])

    # Verify result
    assert result is False


@patch("google.cloud.storage.Client")
def test_get_storage_client(mock_client_class):
    """Test getting a storage client"""
    # Set up mock
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    # Call the function
    client = _get_storage_client()

    # Verify
    assert client == mock_client
    mock_client_class.assert_called_once()


@patch("pipelines.scrapping_dest_details.gcs_storage._get_storage_client")
@patch("pipelines.scrapping_dest_details.gcs_storage.BUCKET_NAME", "test-bucket")
def test_get_bucket(mock_get_client):
    """Test getting a GCS bucket"""
    # Set up mock
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_get_client.return_value = mock_client
    mock_client.bucket.return_value = mock_bucket

    # Call the function
    bucket = _get_bucket()

    # Verify
    assert bucket == mock_bucket
    mock_client.bucket.assert_called_once_with("test-bucket")
