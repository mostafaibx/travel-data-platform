import datetime
import json
from unittest.mock import MagicMock, patch

import pytest

from ..config import GCS_BUCKET_NAME
from ..gcs_storage import (
    upload_processed_wiki_data,
    upload_raw_wiki_data,
)


@pytest.fixture
def sample_raw_wiki_data():
    """Sample raw Wikipedia data for testing"""
    return {
        "title": "Paris",
        "content": "Paris is the capital of France",
        "sections": {
            "Geography": "Paris is located in northern France",
            "History": "Paris has a rich history dating back to Roman times",
        },
        "infobox": {
            "Country": "France",
            "Population": "2,161,000 (2019)",
        },
    }


@pytest.fixture
def sample_processed_wiki_data():
    """Sample processed Wikipedia data for testing"""
    return [
        {
            "destination_name": "Paris",
            "description": "Paris is the capital of France",
            "country": "France",
            "population_count": 2161000,
            "population_year": 2019,
            "latitude": 48.8566,
            "longitude": 2.3522,
        },
        {
            "destination_name": "London",
            "description": "London is the capital of the United Kingdom",
            "country": "United Kingdom",
            "population_count": 8982000,
            "population_year": 2019,
            "latitude": 51.5074,
            "longitude": -0.1278,
        },
    ]


@patch("google.cloud.storage.Client")
def test_get_storage_client(mock_client_class):
    """Test _get_storage_client creates and returns a storage client"""
    from ..gcs_storage import _get_storage_client

    # Arrange
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    # Act
    client = _get_storage_client()

    # Assert
    assert client is mock_client
    mock_client_class.assert_called_once()


@patch("google.cloud.storage.Client")
def test_get_bucket(mock_client_class):
    """Test _get_bucket returns the correct bucket"""
    from ..gcs_storage import _get_bucket

    # Arrange
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_bucket = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    # Act
    bucket = _get_bucket()

    # Assert
    assert bucket is mock_bucket
    mock_client.bucket.assert_called_once_with(GCS_BUCKET_NAME)


@patch("google.cloud.storage.Client")
def test_upload_raw_wiki_data_success(mock_client_class):
    """Test upload_raw_wiki_data successfully uploads the data"""
    # Arrange
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_bucket = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob

    # Set up the datetime patch
    with patch("datetime.datetime") as mock_datetime:
        mock_now = MagicMock()
        mock_datetime.now.return_value = mock_now
        mock_now.strftime.side_effect = lambda fmt: (
            "2023/01/01" if fmt == "%Y/%m/%d" else "20230101_120000"
        )

        # Test data
        destination = "Paris"
        data = {"title": "Paris", "content": "Beautiful city"}

        # Act
        result = upload_raw_wiki_data(destination, data)

        # Assert
        assert result is True
        expected_blob_path = "raw/wiki/2023/01/01/Paris_20230101_120000.json"
        mock_bucket.blob.assert_called_once_with(expected_blob_path)
        mock_blob.upload_from_string.assert_called_once()

        # Check JSON was formatted correctly
        call_args = mock_blob.upload_from_string.call_args[0]
        uploaded_data = json.loads(call_args[0])
        assert uploaded_data == data
        assert (
            mock_blob.upload_from_string.call_args[1]["content_type"]
            == "application/json"
        )


@patch("google.cloud.storage.Client")
def test_upload_raw_wiki_data_with_spaces(mock_client_class):
    """Test upload_raw_wiki_data handles destination names with spaces"""
    # Arrange
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_bucket = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob

    # Set up the datetime patch
    with patch("datetime.datetime") as mock_datetime:
        mock_now = MagicMock()
        mock_datetime.now.return_value = mock_now
        mock_now.strftime.side_effect = lambda fmt: (
            "2023/01/01" if fmt == "%Y/%m/%d" else "20230101_120000"
        )

        # Test data
        destination = "New York City"
        data = {"title": "New York City", "content": "The Big Apple"}

        # Act
        result = upload_raw_wiki_data(destination, data)

        # Assert
        assert result is True
        expected_blob_path = "raw/wiki/2023/01/01/New_York_City_20230101_120000.json"
        mock_bucket.blob.assert_called_once_with(expected_blob_path)


@patch("google.cloud.storage.Client")
def test_upload_raw_wiki_data_exception(mock_client_class):
    """Test upload_raw_wiki_data handles exceptions properly"""
    # Arrange
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    # Simulate an exception when getting the bucket
    mock_client.bucket.side_effect = Exception("Bucket access error")

    # Test data
    destination = "Paris"
    data = {"title": "Paris", "content": "Beautiful city"}

    # Act
    result = upload_raw_wiki_data(destination, data)

    # Assert
    assert result is False


@patch("google.cloud.storage.Client")
def test_upload_processed_wiki_data_success(
    mock_client_class, sample_processed_wiki_data
):
    """Test upload_processed_wiki_data successfully uploads the data"""
    # Arrange
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_bucket = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob

    # Set up the datetime patch
    with patch("datetime.datetime") as mock_datetime:
        mock_now = MagicMock()
        mock_datetime.now.return_value = mock_now
        mock_now.strftime.return_value = "20230101_120000"

        # Act
        result = upload_processed_wiki_data(sample_processed_wiki_data)

        # Assert
        assert result is True
        expected_blob_path = "processed/wiki/destinations_20230101_120000.json"
        mock_bucket.blob.assert_called_once_with(expected_blob_path)
        mock_blob.upload_from_string.assert_called_once()

        # Check JSON was formatted correctly
        call_args = mock_blob.upload_from_string.call_args[0]
        uploaded_data = json.loads(call_args[0])
        assert uploaded_data == sample_processed_wiki_data
        assert (
            mock_blob.upload_from_string.call_args[1]["content_type"]
            == "application/json"
        )


@patch("google.cloud.storage.Client")
def test_upload_processed_wiki_data_with_datetime(mock_client_class):
    """Test upload_processed_wiki_data handles datetime objects in the data"""
    # Arrange
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    mock_bucket = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob

    # Create data with a datetime field
    data = [
        {
            "destination_name": "Paris",
            "timestamp": datetime.datetime(2023, 1, 1, 12, 0, 0),
        }
    ]

    # Set up the datetime patch for the function
    with patch("datetime.datetime") as mock_datetime:
        mock_now = MagicMock()
        mock_datetime.now.return_value = mock_now
        mock_now.strftime.return_value = "20230101_120000"

        # Act
        result = upload_processed_wiki_data(data)

        # Assert
        assert result is True
        mock_blob.upload_from_string.assert_called_once()

        # Check datetime was serialized properly
        call_args = mock_blob.upload_from_string.call_args[0]
        uploaded_data = json.loads(call_args[0])
        assert uploaded_data[0]["timestamp"] == "2023-01-01 12:00:00"


@patch("google.cloud.storage.Client")
def test_upload_processed_wiki_data_exception(mock_client_class):
    """Test upload_processed_wiki_data handles exceptions properly"""
    # Arrange
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    # Simulate an exception when getting the bucket
    mock_client.bucket.side_effect = Exception("Bucket access error")

    # Test data
    data = [{"destination_name": "Paris"}]

    # Act
    result = upload_processed_wiki_data(data)

    # Assert
    assert result is False
