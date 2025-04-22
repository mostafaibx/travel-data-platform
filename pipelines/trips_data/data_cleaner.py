import pandas as pd
import numpy as np
import os
import logging
from pathlib import Path
import glob
from datetime import datetime

from pipelines.trips_data import config

# Set up logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_cleaner')

class TravelDataCleaner:
    """Cleans raw travel data for analysis and BigQuery ingestion."""
    
    def __init__(self):
        """Initialize the data cleaner."""
        logger.info("TravelDataCleaner initialized")
    
    def clean_data(self, df):
        """Clean and standardize a travel data DataFrame."""
        # Make a copy to avoid modifying the original
        df_cleaned = df.copy()
        
        # Handle missing values
        df_cleaned = df_cleaned.dropna(how='all')
        
        # Convert cost columns to numeric
        for col in ['Accommodation cost', 'Transportation cost']:
            df_cleaned[col] = df_cleaned[col].apply(
                lambda x: float(str(x).replace('$', '').replace(',', '').replace('USD', '').strip()) 
                if pd.notnull(x) and str(x).strip() else np.nan
            )
        
        # Ensure durations are calculated correctly
        df_cleaned['Duration (days)'] = pd.to_numeric(
            df_cleaned['Duration (days)'], errors='coerce'
        ).fillna(7)  # Default to 7 days if invalid
        
        # Convert traveler age to numeric
        df_cleaned['Traveler age'] = pd.to_numeric(
            df_cleaned['Traveler age'], errors='coerce'
        )
        
        # Standardize date formats
        for date_col in ['Start date', 'End date']:
            df_cleaned[date_col] = pd.to_datetime(
                df_cleaned[date_col], errors='coerce'
            ).dt.strftime('%Y-%m-%d')
        
        # Fill any remaining NaN values
        df_cleaned = df_cleaned.fillna({
            'Traveler gender': 'Unknown',
            'Traveler nationality': 'Unknown',
            'Accommodation type': 'Unknown',
            'Transportation type': 'Unknown'
        })
        
        logger.debug(f"Cleaned data with {len(df_cleaned)} records")
        return df_cleaned
    
    def clean_file(self, input_filepath, output_filepath=None):
        """Clean a raw data file and save the cleaned version."""
        # Load the raw data
        df_raw = pd.read_csv(input_filepath)
        logger.info(f"Loaded raw data from {input_filepath} with {len(df_raw)} records")
        
        # Clean the data
        df_cleaned = self.clean_data(df_raw)
        
        # Generate output path if not provided
        if output_filepath is None:
            # Use the input filename but change the directory and add '_cleaned'
            input_filename = os.path.basename(input_filepath)
            name_parts = os.path.splitext(input_filename)
            output_filename = f"{name_parts[0]}_cleaned{name_parts[1]}"
            output_filepath = config.PROCESSED_DATA_DIR / output_filename
        
        # Save the cleaned data
        df_cleaned.to_csv(output_filepath, index=False)
        logger.info(f"Saved cleaned data to {output_filepath}")
        
        return output_filepath
    
    def clean_latest_raw_file(self):
        """Find and clean the most recent raw data file."""
        # Get a list of all raw data files
        raw_files = glob.glob(str(config.RAW_DATA_DIR / "*travel_data_*.csv"))
        
        if not raw_files:
            logger.warning("No raw data files found")
            return None
        
        # Sort files by name/date (assuming they follow the naming convention)
        raw_files.sort()
        latest_file = raw_files[-1]
        
        # Clean the file
        return self.clean_file(latest_file)


def clean_latest_data():
    """Clean the latest raw travel data file."""
    cleaner = TravelDataCleaner()
    output_path = cleaner.clean_latest_raw_file()
    return output_path


if __name__ == "__main__":
    clean_latest_data() 