import os


WEATHER_API_KEY = "55454a8d3a13e4d28301e18f43e0af1a"
BASE_URL = "https://api.openweathermap.org/data/3.0/onecall"


# BigQuery settings
PROJECT_ID = "travler-data-platform"
DATASET_ID = "staging"
TABLE_ID = "weather_data"
BQ_TABLE_PATH = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Cloud Storage settings
BUCKET_NAME = "travel-data-platform-raw"
WEATHER_RAW_PATH = "weather_data/raw"