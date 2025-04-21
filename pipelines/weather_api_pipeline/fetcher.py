import requests
from pipelines.fetch_weather_api.config import WEATHER_API_KEY, BASE_URL

class WeatherFetcher:
    def fetch(self, city: str) -> dict:
        params = {
            "q": city,
            "appid": WEATHER_API_KEY,
            "units": "metric"
        }
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        return response.json()