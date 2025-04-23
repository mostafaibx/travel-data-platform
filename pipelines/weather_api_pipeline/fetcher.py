import logging
from typing import Any, Dict, List, Optional

import requests

from .config import BASE_URL, WEATHER_API_KEY

logger = logging.getLogger(__name__)


def get_weather_data(
    lat: float,
    lon: float,
    exclude: Optional[List[str]] = None,
    units: str = "metric",
    lang: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch weather data from OpenWeather API for a specific location.

    Args:
        lat: Latitude of the location (-90 to 90)
        lon: Longitude of the location (-180 to 180)
        exclude: List of data blocks to exclude (current, minutely, hourly, daily, alerts)
        units: Units of measurement. Available options: standard, metric, imperial
        lang: Language for output data

    Returns:
        Weather data as dictionary
    """
    params = {"lat": lat, "lon": lon, "appid": WEATHER_API_KEY, "units": units}

    if exclude:
        params["exclude"] = ",".join(exclude)

    if lang:
        params["lang"] = lang

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching weather data: {e}")
        raise
