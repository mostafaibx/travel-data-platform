"""
Tests for the BigQuery loader module in the scrapping_dest_details pipeline.
"""

from unittest.mock import MagicMock, call, patch

from ..bigquery_loader import BigQueryLoader


class TestBigQueryLoader:
    """Tests for the BigQueryLoader class"""

    def test_init(self):
        """Test initialization of the BigQueryLoader"""
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")

        assert loader.project_id == "test-project"
        assert loader.dataset_id == "test-dataset"
        assert loader.table_id == "test-table"
        assert loader.table_path == "test-project.test-dataset.test-table"

    @patch("pipelines.scrapping_dest_details.bigquery_loader.bigquery.Client")
    def test_get_client(self, mock_client_class):
        """Test getting a BigQuery client"""
        # Set up mock
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Create loader and call the method
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        client = loader._get_client()

        # Verify
        assert client == mock_client
        mock_client_class.assert_called_once_with(project="test-project")

    @patch("pipelines.scrapping_dest_details.bigquery_loader.bigquery.Client")
    def test_check_dataset_exists(self, mock_client_class):
        """Test checking if a dataset exists"""
        # Set up mock client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock dataset_exists method to return True
        mock_client.get_dataset.return_value = True

        # Create loader and call the method
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        exists = loader._check_dataset_exists()

        # Verify
        assert exists is True
        mock_client.get_dataset.assert_called_once()

    @patch("pipelines.scrapping_dest_details.bigquery_loader.bigquery.Client")
    def test_check_dataset_not_exists(self, mock_client_class):
        """Test checking if a dataset doesn't exist"""
        # Set up mock client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock dataset_exists method to raise NotFound
        from google.cloud.exceptions import NotFound

        mock_client.get_dataset.side_effect = NotFound("Not found")

        # Create loader and call the method
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        exists = loader._check_dataset_exists()

        # Verify
        assert exists is False
        mock_client.get_dataset.assert_called_once()

    @patch("pipelines.scrapping_dest_details.bigquery_loader.bigquery.Client")
    def test_check_table_exists(self, mock_client_class):
        """Test checking if a table exists"""
        # Set up mock client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock table_exists method to return True
        mock_client.get_table.return_value = True

        # Create loader and call the method
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        exists = loader._check_table_exists()

        # Verify
        assert exists is True
        mock_client.get_table.assert_called_once()

    @patch("pipelines.scrapping_dest_details.bigquery_loader.bigquery.Client")
    def test_check_table_not_exists(self, mock_client_class):
        """Test checking if a table doesn't exist"""
        # Set up mock client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock table_exists method to raise NotFound
        from google.cloud.exceptions import NotFound

        mock_client.get_table.side_effect = NotFound("Not found")

        # Create loader and call the method
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        exists = loader._check_table_exists()

        # Verify
        assert exists is False
        mock_client.get_table.assert_called_once()

    @patch("pipelines.scrapping_dest_details.bigquery_loader.bigquery.Client")
    @patch("pipelines.scrapping_dest_details.bigquery_loader.bigquery.DatasetReference")
    def test_create_dataset(self, mock_dataset_ref, mock_client_class):
        """Test creating a dataset"""
        # Set up mocks
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_dataset = MagicMock()
        mock_client.create_dataset.return_value = mock_dataset

        # Create loader and call the method
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        result = loader._create_dataset()

        # Verify
        assert result is True
        mock_client.create_dataset.assert_called_once()

    @patch("pipelines.scrapping_dest_details.bigquery_loader.bigquery.Client")
    def test_create_dataset_failure(self, mock_client_class):
        """Test handling failure when creating a dataset"""
        # Set up mock to raise exception
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.create_dataset.side_effect = Exception("Creation error")

        # Create loader and call the method
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        result = loader._create_dataset()

        # Verify
        assert result is False
        mock_client.create_dataset.assert_called_once()

    @patch("pipelines.scrapping_dest_details.bigquery_loader.bigquery.Client")
    @patch("pipelines.scrapping_dest_details.bigquery_loader.bigquery.LoadJobConfig")
    def test_upload_dataframe(
        self, mock_job_config, mock_client_class, sample_processed_df
    ):
        """Test uploading a DataFrame to BigQuery"""
        # Set up mocks
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_config = MagicMock()
        mock_job_config.return_value = mock_config
        mock_job = MagicMock()
        mock_client.load_table_from_dataframe.return_value = mock_job
        mock_job.result.return_value = None

        # Create loader and call the method
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        result = loader._upload_dataframe(sample_processed_df)

        # Verify
        assert result is True
        mock_client.load_table_from_dataframe.assert_called_once_with(
            sample_processed_df,
            destination="test-project.test-dataset.test-table",
            job_config=mock_config,
        )
        mock_job.result.assert_called_once()

    @patch("pipelines.scrapping_dest_details.bigquery_loader.bigquery.Client")
    @patch("pipelines.scrapping_dest_details.bigquery_loader.bigquery.LoadJobConfig")
    def test_upload_dataframe_failure(
        self, mock_job_config, mock_client_class, sample_processed_df
    ):
        """Test handling failure when uploading a DataFrame"""
        # Set up mocks
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_config = MagicMock()
        mock_job_config.return_value = mock_config
        mock_job = MagicMock()
        mock_client.load_table_from_dataframe.return_value = mock_job
        mock_job.result.side_effect = Exception("Upload error")

        # Create loader and call the method
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        result = loader._upload_dataframe(sample_processed_df)

        # Verify
        assert result is False

    @patch.object(BigQueryLoader, "_check_dataset_exists")
    @patch.object(BigQueryLoader, "_create_dataset")
    @patch.object(BigQueryLoader, "_check_table_exists")
    @patch.object(BigQueryLoader, "_upload_dataframe")
    def test_upload_new_table_success(
        self,
        mock_upload,
        mock_check_table,
        mock_create_dataset,
        mock_check_dataset,
        sample_processed_df,
    ):
        """Test uploading a DataFrame to a new table"""
        # Configure mocks for success path
        mock_check_dataset.return_value = True
        mock_check_table.return_value = False
        mock_upload.return_value = True

        # Create loader and call the method
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        result = loader.upload_new_table(sample_processed_df)

        # Verify
        assert result is True
        mock_check_dataset.assert_called_once()
        mock_check_table.assert_called_once()
        mock_upload.assert_called_once_with(sample_processed_df)
        mock_create_dataset.assert_not_called()  # Dataset exists

    @patch.object(BigQueryLoader, "_check_dataset_exists")
    @patch.object(BigQueryLoader, "_create_dataset")
    @patch.object(BigQueryLoader, "_check_table_exists")
    @patch.object(BigQueryLoader, "_upload_dataframe")
    def test_upload_new_table_create_dataset(
        self,
        mock_upload,
        mock_check_table,
        mock_create_dataset,
        mock_check_dataset,
        sample_processed_df,
    ):
        """Test uploading a DataFrame to a new table with dataset creation"""
        # Configure mocks for dataset creation path
        mock_check_dataset.return_value = False
        mock_create_dataset.return_value = True
        mock_check_table.return_value = False
        mock_upload.return_value = True

        # Create loader and call the method
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        result = loader.upload_new_table(sample_processed_df)

        # Verify
        assert result is True
        mock_check_dataset.assert_called_once()
        mock_create_dataset.assert_called_once()
        mock_check_table.assert_called_once()
        mock_upload.assert_called_once_with(sample_processed_df)

    @patch.object(BigQueryLoader, "_check_dataset_exists")
    @patch.object(BigQueryLoader, "_check_table_exists")
    def test_upload_new_table_table_exists(
        self, mock_check_table, mock_check_dataset, sample_processed_df
    ):
        """Test handling case where the table already exists"""
        # Configure mocks for table exists path
        mock_check_dataset.return_value = True
        mock_check_table.return_value = True

        # Create loader and call the method
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")
        result = loader.upload_new_table(sample_processed_df)

        # Verify
        assert result is False  # Should fail if table exists
        mock_check_dataset.assert_called_once()
        mock_check_table.assert_called_once()

    @patch("pipelines.scrapping_dest_details.bigquery_loader.bigquery.Client")
    def test_upload_with_merge(self, mock_client_class, sample_processed_df):
        """Test uploading a DataFrame with merge operation"""
        # Set up mock client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_query_job.result.return_value = None

        # Create loader
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")

        # Mock temp table creation
        with patch.object(loader, "upload_new_table") as mock_upload_new:
            mock_upload_new.return_value = True

            # Call the method
            result = loader.upload_with_merge(sample_processed_df)

            # Verify
            assert result is True

            # Check that a temp table was created with our data
            mock_upload_new.assert_called_once()

            # Check that the merge query was executed
            mock_client.query.assert_called_once()
            query_arg = mock_client.query.call_args[0][0]
            assert "MERGE" in query_arg
            assert "test-project.test-dataset.test-table" in query_arg
            mock_query_job.result.assert_called_once()

    @patch("pipelines.scrapping_dest_details.bigquery_loader.bigquery.Client")
    def test_upload_with_merge_temp_table_failure(
        self, mock_client_class, sample_processed_df
    ):
        """Test handling failure in temp table creation during merge"""
        # Create loader
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")

        # Mock temp table creation failure
        with patch.object(loader, "upload_new_table") as mock_upload_new:
            mock_upload_new.return_value = False

            # Call the method
            result = loader.upload_with_merge(sample_processed_df)

            # Verify failure
            assert result is False
            mock_upload_new.assert_called_once()

    @patch("pipelines.scrapping_dest_details.bigquery_loader.bigquery.Client")
    def test_upload_with_merge_query_failure(
        self, mock_client_class, sample_processed_df
    ):
        """Test handling failure in merge query execution"""
        # Set up mock client with query failure
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_query_job = MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_query_job.result.side_effect = Exception("Query error")

        # Create loader
        loader = BigQueryLoader("test-project", "test-dataset", "test-table")

        # Mock temp table creation success
        with patch.object(loader, "upload_new_table") as mock_upload_new:
            mock_upload_new.return_value = True

            # Call the method
            result = loader.upload_with_merge(sample_processed_df)

            # Verify failure
            assert result is False
            mock_upload_new.assert_called_once()
            mock_client.query.assert_called_once()
            mock_query_job.result.assert_called_once()
