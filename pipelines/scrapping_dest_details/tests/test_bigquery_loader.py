from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from google.cloud import bigquery

from ..bigquery_loader import BigQueryLoader


@pytest.fixture
def sample_destinations_df():
    """Create a sample dataframe with destination data for testing"""
    return pd.DataFrame(
        {
            "destination_name": ["Paris", "London"],
            "description": ["City of lights", "Capital of UK"],
            "country": ["France", "United Kingdom"],
            "latitude": [48.8566, 51.5074],
            "longitude": [2.3522, -0.1278],
            "population_count": [2161000, 8982000],
            "population_year": [2019, 2019],
            "timezone": ["Europe/Paris", "Europe/London"],
            "languages": ['["French"]', '["English"]'],
            "climate": ["Oceanic", "Temperate"],
            "image_url": [
                "http://example.com/paris.jpg",
                "http://example.com/london.jpg",
            ],
            "sections": ['["History", "Culture"]', '["History", "Economy"]'],
            "area_km2": [105.4, 1572.0],
            "region": ["Île-de-France", "Greater London"],
            "attractions_count": [50, 45],
            "attractions": [
                '["Eiffel Tower", "Louvre"]',
                '["Big Ben", "Tower Bridge"]',
            ],
            "ingestion_timestamp": [datetime.now(), datetime.now()],
        }
    )


@pytest.fixture
def mock_bigquery_client():
    """Create a mock BigQuery client"""
    client = MagicMock()
    # Mock dataset function to return another mock
    dataset_mock = MagicMock()
    client.dataset.return_value = dataset_mock

    # Mock table function that returns a table reference mock
    table_mock = MagicMock()
    dataset_mock.table.return_value = table_mock

    return client


@patch("google.cloud.bigquery.Client")
def test_bigquery_loader_init(mock_client_class):
    """Test BigQueryLoader initialization"""
    # Arrange
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client

    # Act
    loader = BigQueryLoader(
        project_id="test-project",
        dataset_id="test_dataset",
        table_id="test_table",
        location="US",
    )

    # Assert
    assert loader.project_id == "test-project"
    assert loader.dataset_id == "test_dataset"
    assert loader.table_id == "test_table"
    assert loader.location == "US"
    assert loader.table_path == "test-project.test_dataset.test_table"
    mock_client_class.assert_called_once_with(project="test-project")


@patch("google.cloud.bigquery.Client")
def test_ensure_dataset_exists_when_exists(mock_client_class):
    """Test _ensure_dataset_exists when dataset already exists"""
    # Arrange
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    loader = BigQueryLoader("test-project", "test_dataset", "test_table")

    # Act
    loader._ensure_dataset_exists()

    # Assert
    mock_client.get_dataset.assert_called_once_with("test_dataset")
    mock_client.create_dataset.assert_not_called()


@patch("google.cloud.bigquery.Client")
def test_ensure_dataset_exists_when_not_exists(mock_client_class):
    """Test _ensure_dataset_exists when dataset doesn't exist"""
    # Arrange
    mock_client = MagicMock()
    mock_client.get_dataset.side_effect = Exception("Dataset not found")
    mock_client_class.return_value = mock_client

    loader = BigQueryLoader("test-project", "test_dataset", "test_table", "EU")

    # Act
    loader._ensure_dataset_exists()

    # Assert
    mock_client.get_dataset.assert_called_once()
    mock_client.create_dataset.assert_called_once()
    # Check that the created dataset has correct properties
    dataset_arg = mock_client.create_dataset.call_args[0][0]
    assert dataset_arg.dataset_id == "test_dataset"
    assert dataset_arg.location == "EU"


@patch("google.cloud.bigquery.Client")
def test_get_schema(mock_client_class):
    """Test get_schema returns correct schema fields"""
    # Arrange
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    loader = BigQueryLoader("test-project", "test_dataset", "test_table")

    # Act
    schema = loader.get_schema()

    # Assert
    assert len(schema) == 17  # Check we have all 17 fields
    assert isinstance(schema[0], bigquery.SchemaField)
    # Check a few specific fields
    assert schema[0].name == "destination_name"
    assert schema[0].field_type == "STRING"
    assert schema[4].name == "longitude"
    assert schema[4].field_type == "FLOAT"
    assert schema[5].name == "population_count"
    assert schema[5].field_type == "INTEGER"
    assert schema[16].name == "ingestion_timestamp"
    assert schema[16].field_type == "TIMESTAMP"


@patch("google.cloud.bigquery.Client")
def test_ensure_table_exists(mock_client_class, mock_bigquery_client):
    """Test _ensure_table_exists creates table if needed"""
    # Arrange
    mock_client_class.return_value = mock_bigquery_client
    loader = BigQueryLoader("test-project", "test_dataset", "test_table")

    # Act
    loader._ensure_table_exists()

    # Assert
    mock_bigquery_client.dataset.assert_called_once_with("test_dataset")
    mock_bigquery_client.dataset().table.assert_called_once_with("test_table")
    mock_bigquery_client.create_table.assert_called_once()
    # Verify schema was passed correctly
    table_arg = mock_bigquery_client.create_table.call_args[0][0]
    assert len(table_arg.schema) == 17


@patch("google.cloud.bigquery.Client")
def test_create_temp_table(
    mock_client_class, mock_bigquery_client, sample_destinations_df
):
    """Test _create_temp_table creates and loads data to a temp table"""
    # Arrange
    mock_client_class.return_value = mock_bigquery_client
    # Mock delete_table to simulate both cases (table exists and doesn't)
    mock_bigquery_client.delete_table.side_effect = [None, Exception("Table not found")]

    loader = BigQueryLoader("test-project", "test_dataset", "test_table")

    # Act
    temp_table_id = loader._create_temp_table(sample_destinations_df)

    # Assert
    assert temp_table_id == "test_table_temp"
    assert mock_bigquery_client.delete_table.call_count == 1
    assert mock_bigquery_client.create_table.call_count == 1
    assert mock_bigquery_client.load_table_from_dataframe.call_count == 1


@patch("google.cloud.bigquery.Client")
def test_build_merge_query(mock_client_class):
    """Test _build_merge_query builds correct SQL query"""
    # Arrange
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    loader = BigQueryLoader("test-project", "test_dataset", "test_table")
    temp_table_id = "test_table_temp"

    # Act
    query = loader._build_merge_query(temp_table_id)

    # Assert
    assert "MERGE" in query
    assert "`test-project.test_dataset.test_table`" in query
    assert "`test-project.test_dataset.test_table_temp`" in query
    assert "target.destination_name = source.destination_name" in query
    # Check some of the updated fields are in the query
    assert "description = source.description" in query
    assert "country = source.country" in query
    assert "ingestion_timestamp = source.ingestion_timestamp" in query


@patch("google.cloud.bigquery.Client")
def test_execute_merge(mock_client_class, mock_bigquery_client):
    """Test _execute_merge executes SQL and cleans up temp table"""
    # Arrange
    mock_client_class.return_value = mock_bigquery_client
    # Set up the returned table with row count
    mock_table = MagicMock()
    mock_table.num_rows = 10
    mock_bigquery_client.get_table.return_value = mock_table

    loader = BigQueryLoader("test-project", "test_dataset", "test_table")

    # Act
    with patch.object(loader, "_build_merge_query", return_value="MERGE QUERY"):
        num_rows = loader._execute_merge("test_table_temp")

    # Assert
    assert num_rows == 10
    mock_bigquery_client.query.assert_called_once_with("MERGE QUERY")
    mock_bigquery_client.delete_table.assert_called_once()


@patch("google.cloud.bigquery.Client")
def test_upload_with_merge_success(
    mock_client_class, mock_bigquery_client, sample_destinations_df
):
    """Test upload_with_merge for successful case"""
    # Arrange
    mock_client_class.return_value = mock_bigquery_client
    loader = BigQueryLoader("test-project", "test_dataset", "test_table")

    # Mock internal methods
    with (
        patch.object(loader, "_ensure_dataset_exists") as mock_ensure_dataset,
        patch.object(loader, "_ensure_table_exists") as mock_ensure_table,
        patch.object(
            loader, "_create_temp_table", return_value="test_table_temp"
        ) as mock_create_temp,
        patch.object(loader, "_execute_merge", return_value=10) as mock_execute_merge,
    ):

        # Act
        result = loader.upload_with_merge(sample_destinations_df)

        # Assert
        assert result is True
        mock_ensure_dataset.assert_called_once()
        mock_ensure_table.assert_called_once()
        mock_create_temp.assert_called_once_with(sample_destinations_df)
        mock_execute_merge.assert_called_once_with("test_table_temp")


@patch("google.cloud.bigquery.Client")
def test_upload_with_merge_empty_df(mock_client_class, mock_bigquery_client):
    """Test upload_with_merge with empty dataframe"""
    # Arrange
    mock_client_class.return_value = mock_bigquery_client
    loader = BigQueryLoader("test-project", "test_dataset", "test_table")
    empty_df = pd.DataFrame()

    # Mock internal methods
    with (
        patch.object(loader, "_ensure_dataset_exists") as mock_ensure_dataset,
        patch.object(loader, "_ensure_table_exists") as mock_ensure_table,
        patch.object(loader, "_create_temp_table") as mock_create_temp,
        patch.object(loader, "_execute_merge") as mock_execute_merge,
    ):

        # Act
        result = loader.upload_with_merge(empty_df)

        # Assert
        assert result is False
        mock_ensure_dataset.assert_called_once()
        mock_ensure_table.assert_called_once()
        mock_create_temp.assert_not_called()
        mock_execute_merge.assert_not_called()


@patch("google.cloud.bigquery.Client")
def test_upload_with_merge_exception(
    mock_client_class, mock_bigquery_client, sample_destinations_df
):
    """Test upload_with_merge handles exceptions"""
    # Arrange
    mock_client_class.return_value = mock_bigquery_client
    loader = BigQueryLoader("test-project", "test_dataset", "test_table")

    # Mock internal methods to raise exception
    with patch.object(
        loader, "_ensure_dataset_exists", side_effect=Exception("Test error")
    ):

        # Act
        result = loader.upload_with_merge(sample_destinations_df)

        # Assert
        assert result is False


@patch("google.cloud.bigquery.Client")
def test_append_data_success(
    mock_client_class, mock_bigquery_client, sample_destinations_df
):
    """Test append_data for successful case"""
    # Arrange
    mock_client_class.return_value = mock_bigquery_client
    loader = BigQueryLoader("test-project", "test_dataset", "test_table")

    # Mock internal methods
    with (
        patch.object(loader, "_ensure_dataset_exists") as mock_ensure_dataset,
        patch.object(loader, "_ensure_table_exists") as mock_ensure_table,
    ):

        # Act
        result = loader.append_data(sample_destinations_df)

        # Assert
        assert result is True
        mock_ensure_dataset.assert_called_once()
        mock_ensure_table.assert_called_once()
        mock_bigquery_client.load_table_from_dataframe.assert_called_once()
        # Check that job config was set to APPEND
        job_config = mock_bigquery_client.load_table_from_dataframe.call_args[1][
            "job_config"
        ]
        assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND


@patch("google.cloud.bigquery.Client")
def test_append_data_empty_df(mock_client_class, mock_bigquery_client):
    """Test append_data with empty dataframe"""
    # Arrange
    mock_client_class.return_value = mock_bigquery_client
    loader = BigQueryLoader("test-project", "test_dataset", "test_table")
    empty_df = pd.DataFrame()

    # Act
    result = loader.append_data(empty_df)

    # Assert
    assert result is False
    mock_bigquery_client.load_table_from_dataframe.assert_not_called()


@patch("google.cloud.bigquery.Client")
def test_append_data_exception(
    mock_client_class, mock_bigquery_client, sample_destinations_df
):
    """Test append_data handles exceptions"""
    # Arrange
    mock_client_class.return_value = mock_bigquery_client
    mock_bigquery_client.load_table_from_dataframe.side_effect = Exception("Load error")
    loader = BigQueryLoader("test-project", "test_dataset", "test_table")

    # Act
    result = loader.append_data(sample_destinations_df)

    # Assert
    assert result is False
