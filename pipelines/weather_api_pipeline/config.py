"""Configuration models for the Weather API pipeline.

Defines Pydantic models for locations and pipeline configuration.
"""
from typing import List
from pydantic import BaseModel, Field, HttpUrl


class Location(BaseModel):
    """Model for location data."""
    city: str
    country: str
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)

class WeatherConfig(BaseModel):
    """Configuration for weather pipeline."""
    project_id: str = Field(..., env="BQ_PROJECT_ID")
    dataset_id: str = Field(..., env="BQ_STAGING_DATASET_ID")
    table_id: str = Field(..., env="BQ_WEATHER_TABLE_ID")
    bucket_name: str = Field(..., env="GCS_BUCKET_NAME")
    weather_raw_path: str | None = Field(None, env="WEATHER_RAW_PATH")
    api_key: str = Field(..., env="WEATHER_API_KEY", description="OpenWeather API key")
    api_base_url: HttpUrl = Field(
        "https://api.openweathermap.org",
        description="OpenWeather API base URL"
    )
    gcs_bucket: str = Field(..., description="GCS bucket for storing weather data")
    forecast_days: int = Field(2, ge=1, le=7, description="Number of days to forecast")
    locations: List[Location] = Field(
        default=[
            Location(city="New York", country="US", latitude=40.7128, longitude=-74.0060),
            Location(city="London", country="UK", latitude=51.5074, longitude=-0.1278),
            Location(city="Paris", country="FR", latitude=48.8566, longitude=2.3522),
            Location(city="Tokyo", country="JP", latitude=35.6762, longitude=139.6503),
            Location(city="Sydney", country="AU", latitude=-33.8688, longitude=151.2093)
        ],
        description="List of locations to fetch weather data for"
    )

    class Config:
        """Pydantic model configuration."""
        frozen = True  # Make config immutable
        extra = "forbid"  # Prevent extra attributes
