"""Weather API pipeline for fetching and storing weather forecast data."""

import logging
from .pipeline import WeatherPipeline
from .config import WeatherConfig


__all__ = ["WeatherPipeline"]


def main():
    """Run the weather API pipeline with proper configuration and error handling"""
    try:
        # Initialize configuration
        weather_config = WeatherConfig()
        
        # Initialize and run pipeline
        pipeline = WeatherPipeline(weather_config)
        success = pipeline.run()
        
        # Exit with appropriate status code
        return 0 if success else 1
        
    except (ValueError, KeyError) as e:
        logging.exception("Failed to run weather pipeline: %s", str(e))
        return 1


if __name__ == "__main__":
    main()
