#!/usr/bin/env python
"""
Entrypoint script to run the data cleaner directly.
This script handles the proper importing of the module and cleaning the latest raw data.
"""

import os
import sys

# Add the project root to Python path to enable imports
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Now we can import from the pipelines package
from pipelines.trips_data.data_cleaner import clean_latest_data

if __name__ == "__main__":
    filepath = clean_latest_data()
    if filepath:
        print(f"Successfully cleaned data and saved to: {filepath}")
    else:
        print("No raw data files found to clean.") 