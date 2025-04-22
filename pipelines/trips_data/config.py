from pathlib import Path
import os

# Base paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Ensure directories exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# Original data file path
ORIGINAL_DATA_FILE = BASE_DIR / "Travel details dataset.csv"

# BigQuery settings
PROJECT_ID = "travler-data-platform"
DATASET_ID = "staging"
TABLE_ID = "trip_details"
BQ_TABLE_PATH = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Fake data generation settings
BATCH_SIZE = 20  # Number of new records to generate per day
DATE_RANGE_FUTURE = 180  # Days into the future for trip dates
PAST_DATE_RATIO = 0.3  # Percentage of trips with past start dates

# Logging config
LOG_LEVEL = "INFO" 