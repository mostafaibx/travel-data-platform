import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# GCP Configuration
PROJECT_ID = os.getenv("BQ_PROJECT_ID")
DATASET_ID = os.getenv("BQ_STAGING_DATASET_ID")
TABLE_ID = os.getenv("BQ_DESTINATION_DETAILS_TABLE_ID")
BQ_TABLE_PATH = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Wikipedia Base URL
WIKIPEDIA_BASE_URL = "https://en.wikipedia.org/wiki/"

# List of popular travel destinations to scrape information for
TRAVEL_DESTINATIONS = [
    "New York City",
    "London",
    "Paris",
    "Tokyo",
    "Sydney",
    "Rome",
    "Bangkok",
    "Dubai",
    "Barcelona",
    "Cancun",
    "Bali",
    "Cape Town",
    "Miami",
    "Cairo",
    "Singapore"
]

# GCS Configuration
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "travel-data-raw")
GCS_WIKI_RAW_PREFIX = "wikipedia_raw" 