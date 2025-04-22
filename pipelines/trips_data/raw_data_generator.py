import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import logging
from pathlib import Path
import os

from pipelines.trips_data import config

# Set up logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('raw_data_generator')

class RawTravelDataGenerator:
    """Generates fake travel data based on patterns in the original dataset without cleaning it first."""
    
    def __init__(self):
        self.df_original = pd.read_csv(config.ORIGINAL_DATA_FILE)
        self.next_trip_id = self.df_original['Trip ID'].max() + 1
        
        # Extract unique values from original data for sampling
        self.destinations = self.df_original['Destination'].dropna().unique()
        self.traveler_names = self.df_original['Traveler name'].dropna().unique()
        self.accommodation_types = self.df_original['Accommodation type'].dropna().unique()
        self.transportation_types = self.df_original['Transportation type'].dropna().unique()
        self.nationalities = self.df_original['Traveler nationality'].dropna().unique()
        
        # Create distributions for sampling (from raw data)
        self.duration_distribution = self._get_duration_distribution()
        self.age_distribution = self._get_age_distribution()
        self.cost_formats = self._get_cost_formats()
        
        logger.info("RawTravelDataGenerator initialized with original data")
    
    def _get_duration_distribution(self):
        """Creates a probability distribution for trip durations."""
        durations = pd.to_numeric(self.df_original['Duration (days)'], errors='coerce').dropna()
        return durations.value_counts(normalize=True).to_dict()
    
    def _get_age_distribution(self):
        """Creates a probability distribution for traveler ages."""
        ages = pd.to_numeric(self.df_original['Traveler age'], errors='coerce').dropna()
        # Group ages into bins
        bins = list(range(18, 70, 5))
        hist, bin_edges = np.histogram(ages, bins=bins)
        probabilities = hist / hist.sum()
        return {
            'bins': bin_edges[:-1],
            'probabilities': probabilities
        }
    
    def _get_cost_formats(self):
        """Extracts the different formats used for costs in the original data."""
        formats = []
        
        for col in ['Accommodation cost', 'Transportation cost']:
            # Get unique formats by examining the data
            unique_formats = []
            for val in self.df_original[col].dropna().astype(str):
                if '$' in val:
                    if ',' in val:
                        unique_formats.append('$NUM_WITH_COMMAS')
                    else:
                        unique_formats.append('$NUM')
                elif 'USD' in val:
                    unique_formats.append('NUM_USD')
                else:
                    # Just a number
                    unique_formats.append('NUM')
            
            formats.extend(unique_formats)
        
        # Calculate probabilities for each format
        format_counts = {}
        for fmt in formats:
            if fmt in format_counts:
                format_counts[fmt] += 1
            else:
                format_counts[fmt] = 1
        
        total = sum(format_counts.values())
        format_probs = {fmt: count/total for fmt, count in format_counts.items()}
        
        return format_probs
    
    def _generate_dates(self):
        """Generate realistic start and end dates for a trip."""
        today = datetime.now().date()
        
        # Decide if this is a past trip or future trip
        is_past_trip = random.random() < config.PAST_DATE_RATIO
        
        if is_past_trip:
            # Generate a trip that started in the past (1-180 days ago)
            days_in_past = random.randint(1, 180)
            start_date = today - timedelta(days=days_in_past)
        else:
            # Generate a trip that will start in the future (1-180 days from now)
            days_in_future = random.randint(1, config.DATE_RANGE_FUTURE)
            start_date = today + timedelta(days=days_in_future)
        
        # Sample from the duration distribution
        items, probabilities = zip(*self.duration_distribution.items())
        duration = int(random.choices(items, weights=probabilities, k=1)[0])
        
        # Calculate end date
        end_date = start_date + timedelta(days=duration)
        
        return start_date, end_date, duration
    
    def _generate_traveler_age(self):
        """Generate a realistic traveler age based on the distribution."""
        bins = self.age_distribution['bins']
        probabilities = self.age_distribution['probabilities']
        bin_idx = np.random.choice(len(bins), p=probabilities)
        base_age = bins[bin_idx]
        # Add some random variation within the bin
        return base_age + random.randint(0, 4)
    
    def _generate_gender(self):
        """Generate a random gender with realistic probabilities."""
        return random.choice(['Male', 'Female'])
    
    def _generate_cost(self):
        """Generate a cost with a random format from those observed in the original data."""
        # Generate a random value between 50 and 3000
        cost_value = round(random.uniform(50, 3000), -1)
        
        # Choose a format based on observed frequencies
        formats = list(self.cost_formats.keys())
        weights = list(self.cost_formats.values())
        chosen_format = random.choices(formats, weights=weights, k=1)[0]
        
        # Format the cost according to the chosen format
        if chosen_format == '$NUM_WITH_COMMAS':
            return f"${cost_value:,.1f}"
        elif chosen_format == '$NUM':
            return f"${cost_value}"
        elif chosen_format == 'NUM_USD':
            return f"{cost_value} USD"
        else:  # 'NUM'
            return cost_value
    
    def generate_trips(self, num_trips=config.BATCH_SIZE):
        """Generate a specified number of fake trip records with raw formats."""
        trips = []
        
        for _ in range(num_trips):
            start_date, end_date, duration = self._generate_dates()
            
            trip = {
                'Trip ID': self.next_trip_id,
                'Destination': random.choice(self.destinations),
                'Start date': start_date.strftime('%-m/%-d/%Y'),
                'End date': end_date.strftime('%-m/%-d/%Y'),
                'Duration (days)': duration,
                'Traveler name': random.choice(self.traveler_names),
                'Traveler age': self._generate_traveler_age(),
                'Traveler gender': self._generate_gender(),
                'Traveler nationality': random.choice(self.nationalities),
                'Accommodation type': random.choice(self.accommodation_types),
                'Accommodation cost': self._generate_cost(),
                'Transportation type': random.choice(self.transportation_types),
                'Transportation cost': self._generate_cost()
            }
            
            trips.append(trip)
            self.next_trip_id += 1
        
        return pd.DataFrame(trips)
    
    def save_to_csv(self, df, filename=None):
        """Save the generated data to a CSV file."""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"raw_travel_data_{timestamp}.csv"
        
        filepath = config.RAW_DATA_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Generated raw data saved to {filepath}")
        return filepath


def generate_daily_raw_data():
    """Generate a new batch of raw travel data for the day."""
    generator = RawTravelDataGenerator()
    df_new = generator.generate_trips()
    timestamp = datetime.now().strftime('%Y%m%d')
    filename = f"raw_travel_data_{timestamp}.csv"
    return generator.save_to_csv(df_new, filename)


if __name__ == "__main__":
    generate_daily_raw_data() 