import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Weather API settings
BASE_URL = os.getenv("WEATHER_BASE_URL")
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")

# BigQuery settings
PROJECT_ID = os.getenv("BQ_PROJECT_ID")
DATASET_ID = os.getenv("BQ_STAGING_DATASET_ID")
TABLE_ID = os.getenv("BQ_WEATHER_TABLE_ID")
BQ_TABLE_PATH = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Cloud Storage settings
BUCKET_NAME = os.getenv("GCS_WEATHER_BUCKET_NAME")
WEATHER_RAW_PATH = os.getenv("WEATHER_RAW_PATH")
