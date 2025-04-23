import logging
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker
from faker.providers import address, date_time, person

from . import config

# Set up logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("faker_data_generator")


class FakerTravelDataGenerator:
    """Generates fake travel data using the Faker library."""

    def __init__(self):
        # Initialize Faker with a single locale first to add providers
        self.faker = Faker("en_US")

        # Add specific providers
        self.faker.add_provider(date_time)
        self.faker.add_provider(person)
        self.faker.add_provider(address)

        # Now create a multi-locale faker for international data
        self.multi_faker = Faker(
            ["en_US", "en_GB", "ja_JP", "es_ES", "de_DE", "fr_FR", "zh_CN"]
        )

        # Get the next trip ID (optionally read from original data if available)
        try:
            df_original = pd.read_csv(config.ORIGINAL_DATA_FILE)
            self.next_trip_id = df_original["Trip ID"].max() + 1
        except:
            self.next_trip_id = 1000

        # Create common travel destinations
        self.destinations = [
            "Paris, France",
            "Tokyo, Japan",
            "New York, USA",
            "London, UK",
            "Rome, Italy",
            "Bali, Indonesia",
            "Barcelona, Spain",
            "Sydney, Australia",
            "Amsterdam, Netherlands",
            "Hong Kong",
            "Dubai, UAE",
            "Bangkok, Thailand",
            "Cancun, Mexico",
            "Rio de Janeiro, Brazil",
            "Cape Town, South Africa",
            "Berlin, Germany",
            "Seoul, South Korea",
            "Marrakech, Morocco",
            "Venice, Italy",
            "Singapore",
            "Phuket, Thailand",
            "Athens, Greece",
            "Vienna, Austria",
            "Kyoto, Japan",
            "Prague, Czech Republic",
            "Lisbon, Portugal",
            "San Francisco, USA",
            "Toronto, Canada",
            "Bora Bora, French Polynesia",
            "Stockholm, Sweden",
        ]

        # Accommodation types
        self.accommodation_types = [
            "Hotel",
            "Hostel",
            "Airbnb",
            "Resort",
            "Villa",
            "Guesthouse",
            "Vacation rental",
            "Apartment",
            "Boutique hotel",
            "Riad",
        ]

        # Transportation types
        self.transportation_types = [
            "Flight",
            "Train",
            "Bus",
            "Car rental",
            "Ferry",
            "Subway",
            "Plane",
            "Taxi",
            "Cruise",
            "Private car",
        ]

        logger.info("FakerTravelDataGenerator initialized")

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

        # Generate a random duration between 3 and 14 days
        duration = random.randint(3, 14)

        # Calculate end date
        end_date = start_date + timedelta(days=duration)

        return start_date, end_date, duration

    def _generate_costs(self):
        """Generate realistic costs for accommodation and transportation with random formatting."""
        # Generate accommodation cost based on different tiers
        accommodation_tier = random.choices(
            ["budget", "mid-range", "luxury", "ultra-luxury"],
            weights=[0.2, 0.5, 0.2, 0.1],
            k=1,
        )[0]

        if accommodation_tier == "budget":
            accommodation_cost_value = random.randint(200, 500)
        elif accommodation_tier == "mid-range":
            accommodation_cost_value = random.randint(500, 1200)
        elif accommodation_tier == "luxury":
            accommodation_cost_value = random.randint(1200, 2000)
        else:  # ultra-luxury
            accommodation_cost_value = random.randint(2000, 5000)

        # Generate transportation cost based on different modes
        transportation_tier = random.choices(
            ["local", "regional", "international", "premium"],
            weights=[0.1, 0.3, 0.5, 0.1],
            k=1,
        )[0]

        if transportation_tier == "local":
            transportation_cost_value = random.randint(50, 200)
        elif transportation_tier == "regional":
            transportation_cost_value = random.randint(200, 500)
        elif transportation_tier == "international":
            transportation_cost_value = random.randint(500, 1000)
        else:  # premium
            transportation_cost_value = random.randint(1000, 2000)

        # Apply random formatting
        accommodation_cost = self._apply_random_format(accommodation_cost_value)
        transportation_cost = self._apply_random_format(transportation_cost_value)

        return accommodation_cost, transportation_cost

    def _apply_random_format(self, cost_value):
        """Apply a random format to the cost value."""
        format_type = random.choices(
            ["raw", "dollar", "dollar_commas", "usd"], weights=[0.3, 0.3, 0.2, 0.2], k=1
        )[0]

        if format_type == "raw":
            return cost_value
        elif format_type == "dollar":
            return f"${cost_value}"
        elif format_type == "dollar_commas":
            return f"${cost_value:,}"
        else:  # usd
            return f"{cost_value} USD"

    def generate_trips(self, num_trips=config.BATCH_SIZE):
        """Generate a specified number of fake trip records using Faker."""
        trips = []

        for _ in range(num_trips):
            # Generate dates
            start_date, end_date, duration = self._generate_dates()

            # Generate demographics
            gender = random.choice(["Male", "Female"])
            if gender == "Male":
                name = self.faker.name_male()
            else:
                name = self.faker.name_female()

            # Generate costs
            accommodation_cost, transportation_cost = self._generate_costs()

            # Create the trip record
            trip = {
                "Trip ID": self.next_trip_id,
                "Destination": random.choice(self.destinations),
                "Start date": start_date.strftime("%-m/%-d/%Y"),
                "End date": end_date.strftime("%-m/%-d/%Y"),
                "Duration (days)": duration,
                "Traveler name": name,
                "Traveler age": random.randint(18, 75),
                "Traveler gender": gender,
                "Traveler nationality": self.multi_faker.random_element(
                    elements=(
                        "American",
                        "British",
                        "Japanese",
                        "Spanish",
                        "German",
                        "French",
                        "Chinese",
                        "Australian",
                        "Canadian",
                        "Italian",
                        "Brazilian",
                        "Mexican",
                        "Korean",
                    )
                ),
                "Accommodation type": random.choice(self.accommodation_types),
                "Accommodation cost": accommodation_cost,
                "Transportation type": random.choice(self.transportation_types),
                "Transportation cost": transportation_cost,
            }

            trips.append(trip)
            self.next_trip_id += 1

        return pd.DataFrame(trips)

    def save_to_csv(self, df, filename=None):
        """Save the generated data to a CSV file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"faker_travel_data_{timestamp}.csv"

        filepath = config.RAW_DATA_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Generated data saved to {filepath}")
        return filepath


def generate_daily_faker_data():
    """Generate a new batch of travel data for the day using Faker."""
    generator = FakerTravelDataGenerator()
    df_new = generator.generate_trips()
    timestamp = datetime.now().strftime("%Y%m%d")
    filename = f"faker_travel_data_{timestamp}.csv"
    return generator.save_to_csv(df_new, filename)


if __name__ == "__main__":
    generate_daily_faker_data()
