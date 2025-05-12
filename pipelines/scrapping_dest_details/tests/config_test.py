"""Test configuration with mock values for testing purposes"""

# Mock GCP Configuration
PROJECT_ID = "test-project"
DATASET_ID = "test_dataset"
TABLE_ID = "test_destinations"
BQ_TABLE_PATH = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Mock Wikipedia Base URL
WIKIPEDIA_BASE_URL = "https://en.wikipedia.org/wiki/"

# Short list of destinations for testing
TRAVEL_DESTINATIONS = ["Paris", "Rome", "New York"]

# Mock GCS Configuration
BUCKET_NAME = "test-bucket"
GCS_BUCKET_NAME = "test-bucket"
GCS_WIKI_RAW_PREFIX = "wikipedia_raw"
