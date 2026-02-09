import requests
from datetime import datetime
from typing import Dict, List, Optional
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_weather_data(
    base_url: str, latitude: float, longitude: float, location_name: str
) -> Optional[Dict]:
    """
    Extract Weather data from Open-Meteo API.

    Args:
        base_url: API base URL
        latitude: Location latitude
        longitude: Location longitude
        location_name: Human-readable location name

    Returns:
        Raw API response as dictionary, or None if failed
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current_weather": True,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "timezone": "auto",
    }

    try:
        logger.info(f"Extracting weather data for {location_name}")
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        data["location_name"] = location_name
        data["extracted_at"] = datetime.utcnow().isoformat()

        logger.info(f"Successfully extracted data for {location_name}")
        return data

    except requests.RequestException as e:
        logger.error(f"Failed to extract data for {location_name}: {e}")
        return None


def extract_all_locations(base_url: str, locations: List[Dict]) -> List[Dict]:
    """
    Extract weather data for multiple locations.

    Args:
        base_url: API base URL
        locations: List of location dictionaries with name, lat, lon

        Returns:
            List of raw API responses
    """
    results = []

    for location in locations:
        data = extract_weather_data(
            base_url=base_url,
            latitude=location["lat"],
            longitude=location["lon"],
            location_name=location["name"],
        )
        if data:
            results.append(data)

    logger.info(f"Extracted data for {len(results)}/{len(locations)} locations")
    return results
