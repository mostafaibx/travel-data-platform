"""OpenWeather API client and data transformer.

Fetches daily forecasts, tracks completeness metrics, and returns both
raw API responses and normalized DataFrame rows.
"""
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple, List
import pandas as pd
import requests

from .config import WeatherConfig

logger = logging.getLogger(__name__)

class WeatherFetcher:
    """Handles fetching weather data from the OpenWeather API."""
    
    def __init__(self, config: WeatherConfig):
        """Initialize weather data fetcher.
        
        Args:
            config: Configuration containing API settings
        """
        self.config = config
        logger.info("Initialized weather fetcher", 
                   extra={"base_url": config.api_base_url, "masked": True})

    def _make_api_request(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        """Make a request to the OpenWeather API.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Optional[Dict[str, Any]]: API response data or None if request failed
        """
        try:
            params = {
                "lat": lat,
                "lon": lon,
                "appid": self.config.api_key,
                "units": "metric",
                "exclude": "minutely,hourly,alerts"
            }
            
            response = requests.get(
                f"{self.config.api_base_url}/data/2.5/onecall",
                params=params,
                timeout=10
            )
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error("API request failed", 
                        extra={
                            "error": str(e),
                            "lat": lat,
                            "lon": lon,
                            "masked": True
                        })
            return None

    def fetch_weather_data(self) -> Optional[Tuple[pd.DataFrame, List[Dict[str, Any]]]]:
        """Fetch weather forecast data and raw payloads for all configured locations.

        Returns a tuple (dataframe, raw_payloads) or None if nothing could be fetched.
        """
        # Note: forecast_date is derived from API dt (UTC), not local 'today'
        data_rows = []
        raw_payloads: List[Dict[str, Any]] = []
        expected_total_rows = len(self.config.locations) * self.config.forecast_days
        total_filled_defaults = {
            "precipitation_probability": 0,
            "rain": 0,
            "uvi": 0,
            "clouds": 0,
        }
        
        for location in self.config.locations:
            try:
                forecast = self._make_api_request(location.latitude, location.longitude)
                if not forecast or "daily" not in forecast:
                    logger.warning(
                        "No daily forecast data available", 
                        extra={
                            "city": location.city,
                            "country": location.country
                        }
                    )
                    continue

                # Keep raw response for lineage
                raw_payloads.append(
                    {
                        "city": location.city,
                        "country": location.country,
                        "latitude": location.latitude,
                        "longitude": location.longitude,
                        "response": forecast,
                    }
                )

                # Process forecast data for configured number of days
                fetched_days_for_location = min(self.config.forecast_days, len(forecast["daily"]))
                for i in range(fetched_days_for_location):
                    day_data = forecast["daily"][i]
                    # Normalize forecast date deterministically from API epoch to UTC date
                    forecast_dt_utc = datetime.fromtimestamp(day_data["dt"], tz=timezone.utc)
                    forecast_date_utc = forecast_dt_utc.date()
                    
                    # Extract weather information
                    pop_val = day_data.get("pop")
                    if pop_val is None:
                        total_filled_defaults["precipitation_probability"] += 1

                    rain_val = day_data.get("rain")
                    if rain_val is None:
                        total_filled_defaults["rain"] += 1

                    uvi_val = day_data.get("uvi")
                    if uvi_val is None:
                        total_filled_defaults["uvi"] += 1

                    clouds_val = day_data.get("clouds")
                    if clouds_val is None:
                        total_filled_defaults["clouds"] += 1

                    weather_row = {
                        "city": location.city,
                        "country": location.country,
                        "latitude": location.latitude,
                        "longitude": location.longitude,
                        "forecast_date": forecast_date_utc,
                        "forecast_timestamp": datetime.fromtimestamp(day_data["dt"]),
                        "max_temp": day_data["temp"]["max"],
                        "min_temp": day_data["temp"]["min"],
                        "day_temp": day_data["temp"]["day"],
                        "night_temp": day_data["temp"]["night"],
                        "feels_like_day": day_data["feels_like"]["day"],
                        "feels_like_night": day_data["feels_like"]["night"],
                        "humidity": day_data["humidity"],
                        "wind_speed": day_data["wind_speed"],
                        "weather_main": day_data["weather"][0]["main"],
                        "weather_description": day_data["weather"][0]["description"],
                        "weather_icon": day_data["weather"][0]["icon"],
                        "precipitation_probability": pop_val if pop_val is not None else 0,
                        "rain": rain_val if rain_val is not None else 0,
                        "uvi": uvi_val if uvi_val is not None else 0,
                        "clouds": clouds_val if clouds_val is not None else 0,
                        "sunrise": datetime.fromtimestamp(day_data["sunrise"]),
                        "sunset": datetime.fromtimestamp(day_data["sunset"]),
                        "ingestion_timestamp": datetime.now(),
                        "logical_key": f"{location.city}|{location.country}|{forecast_date_utc.isoformat()}",
                    }
                    data_rows.append(weather_row)

                # Per-location completeness summary
                logger.info(
                    "Location completeness",
                    extra={
                        "city": location.city,
                        "country": location.country,
                        "expected_days": self.config.forecast_days,
                        "available_days": len(forecast["daily"]),
                        "fetched_days": fetched_days_for_location,
                        "missing_days": max(self.config.forecast_days - fetched_days_for_location, 0),
                    },
                )
                
            except (KeyError, TypeError, ValueError, IndexError, AttributeError) as e:
                logger.exception(
                    "Failed to process forecast data",
                    extra={
                        "error": str(e),
                        "city": location.city,
                        "country": location.country,
                        "masked": True
                    }
                )

        if not data_rows:
            logger.error("No weather data was successfully fetched")
            return None

        df = pd.DataFrame(data_rows)

        # Compute missing percentage for critical fields
        critical_fields = [
            "max_temp",
            "min_temp",
            "humidity",
            "wind_speed",
            "weather_main",
            "forecast_timestamp",
        ]
        missing_pct = {}
        for col in critical_fields:
            if col in df.columns:
                missing_pct[col] = float(df[col].isna().mean() * 100.0)

        coverage_pct = float((len(df) / expected_total_rows) * 100.0) if expected_total_rows else 0.0

        logger.info(
            "Fetch completeness summary",
            extra={
                "expected_rows": expected_total_rows,
                "fetched_rows": len(df),
                "coverage_pct": round(coverage_pct, 2),
                "filled_defaults_counts": total_filled_defaults,
                "missing_pct_by_field": missing_pct,
            },
        )

        return df, raw_payloads
