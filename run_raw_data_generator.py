#!/usr/bin/env python
"""
Entrypoint script to run the raw data generator directly.
This script handles the proper importing of the module and running the generator.
"""

import os
import sys

# Add the project root to Python path to enable imports
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Now we can import from the pipelines package
from pipelines.trips_data.raw_data_generator import generate_daily_raw_data

if __name__ == "__main__":
    filepath = generate_daily_raw_data()
    print(f"Successfully generated raw data and saved to: {filepath}") 