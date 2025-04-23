import logging
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from . import config

# Set up logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("data_generator")


class TravelDataGenerator:
    """Generates fake travel data based on patterns in the original dataset."""

    def __init__(self):
        self.df_original = pd.read_csv(config.ORIGINAL_DATA_FILE)
        self._clean_original_data()
        self.next_trip_id = self.df_original["Trip ID"].max() + 1

        # Extract unique values from original data for sampling
        self.destinations = self.df_original["Destination"].dropna().unique()
        self.traveler_names = self.df_original["Traveler name"].dropna().unique()
        self.accommodation_types = (
            self.df_original["Accommodation type"].dropna().unique()
        )
        self.transportation_types = (
            self.df_original["Transportation type"].dropna().unique()
        )
        self.nationalities = self.df_original["Traveler nationality"].dropna().unique()

        # Create distributions for sampling
        self.duration_distribution = self._get_duration_distribution()
        self.age_distribution = self._get_age_distribution()
        self.accommodation_cost_distribution = self._get_cost_distribution(
            "Accommodation cost"
        )
        self.transportation_cost_distribution = self._get_cost_distribution(
            "Transportation cost"
        )

        logger.info("TravelDataGenerator initialized with original data")

    def _clean_original_data(self):
        """Cleans and standardizes the original dataset."""
        # Handle missing values
        self.df_original = self.df_original.dropna(how="all")

        # Convert cost columns to numeric
        for col in ["Accommodation cost", "Transportation cost"]:
            self.df_original[col] = self.df_original[col].apply(
                lambda x: (
                    float(
                        str(x)
                        .replace("$", "")
                        .replace(",", "")
                        .replace("USD", "")
                        .strip()
                    )
                    if pd.notnull(x) and str(x).strip()
                    else np.nan
                )
            )

        # Ensure durations are calculated correctly
        self.df_original["Duration (days)"] = pd.to_numeric(
            self.df_original["Duration (days)"], errors="coerce"
        ).fillna(
            7
        )  # Default to 7 days if invalid

        logger.debug("Original data cleaned")

    def _get_duration_distribution(self):
        """Creates a probability distribution for trip durations."""
        durations = self.df_original["Duration (days)"].dropna()
        return durations.value_counts(normalize=True).to_dict()

    def _get_age_distribution(self):
        """Creates a probability distribution for traveler ages."""
        ages = self.df_original["Traveler age"].dropna()
        # Group ages into bins
        bins = list(range(18, 70, 5))
        hist, bin_edges = np.histogram(ages, bins=bins)
        probabilities = hist / hist.sum()
        return {"bins": bin_edges[:-1], "probabilities": probabilities}

    def _get_cost_distribution(self, cost_column):
        """Creates a probability distribution for costs."""
        costs = self.df_original[cost_column].dropna()
        # Get quartiles for realistic costs
        q1, median, q3 = costs.quantile([0.25, 0.5, 0.75])
        min_val, max_val = costs.min(), costs.max()
        return {"min": min_val, "q1": q1, "median": median, "q3": q3, "max": max_val}

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
        bins = self.age_distribution["bins"]
        probabilities = self.age_distribution["probabilities"]
        bin_idx = np.random.choice(len(bins), p=probabilities)
        base_age = bins[bin_idx]
        # Add some random variation within the bin
        return base_age + random.randint(0, 4)

    def _generate_gender(self):
        """Generate a random gender with realistic probabilities."""
        return random.choice(["Male", "Female"])

    def _generate_cost(self, distribution):
        """Generate a realistic cost based on the distribution."""
        # Select a quartile with weighted probabilities
        quartile = random.choices(
            ["min_to_q1", "q1_to_median", "median_to_q3", "q3_to_max"],
            weights=[0.2, 0.3, 0.3, 0.2],
            k=1,
        )[0]

        if quartile == "min_to_q1":
            return round(random.uniform(distribution["min"], distribution["q1"]), -1)
        elif quartile == "q1_to_median":
            return round(random.uniform(distribution["q1"], distribution["median"]), -1)
        elif quartile == "median_to_q3":
            return round(random.uniform(distribution["median"], distribution["q3"]), -1)
        else:  # q3_to_max
            return round(random.uniform(distribution["q3"], distribution["max"]), -1)

    def generate_trips(self, num_trips=config.BATCH_SIZE):
        """Generate a specified number of fake trip records."""
        trips = []

        for _ in range(num_trips):
            start_date, end_date, duration = self._generate_dates()

            trip = {
                "Trip ID": self.next_trip_id,
                "Destination": random.choice(self.destinations),
                "Start date": start_date.strftime("%-m/%-d/%Y"),
                "End date": end_date.strftime("%-m/%-d/%Y"),
                "Duration (days)": duration,
                "Traveler name": random.choice(self.traveler_names),
                "Traveler age": self._generate_traveler_age(),
                "Traveler gender": self._generate_gender(),
                "Traveler nationality": random.choice(self.nationalities),
                "Accommodation type": random.choice(self.accommodation_types),
                "Accommodation cost": self._generate_cost(
                    self.accommodation_cost_distribution
                ),
                "Transportation type": random.choice(self.transportation_types),
                "Transportation cost": self._generate_cost(
                    self.transportation_cost_distribution
                ),
            }

            trips.append(trip)
            self.next_trip_id += 1

        return pd.DataFrame(trips)

    def save_to_csv(self, df, filename=None):
        """Save the generated data to a CSV file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"travel_data_{timestamp}.csv"

        filepath = config.RAW_DATA_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Generated data saved to {filepath}")
        return filepath


def generate_daily_data():
    """Generate a new batch of travel data for the day."""
    generator = TravelDataGenerator()
    df_new = generator.generate_trips()
    timestamp = datetime.now().strftime("%Y%m%d")
    filename = f"travel_data_{timestamp}.csv"
    return generator.save_to_csv(df_new, filename)


if __name__ == "__main__":
    generate_daily_data()
