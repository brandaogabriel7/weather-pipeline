import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


def transform_current_weather(raw_data: Dict) -> Dict:
    """
    Transform raw API response into clean current weather record.

    Args:
        raw_data: Raw API response dictionary

    Returns:
        Cleaned weather record
    """
    current = raw_data.get("current_weather", {})

    return {
        "location_name": raw_data.get("location_name"),
        "latitude": raw_data.get("latitude"),
        "longitude": raw_data.get("longitude"),
        "temperature_celsius": current.get("temperature"),
        "wind_speed_kmh": current.get("windspeed"),
        "wind_direction_degrees": current.get("winddirection"),
        "weather_code": current.get("weathercode"),
        "observation_time": current.get("time"),
        "extracted_at": raw_data.get("extracted_at"),
        "loaded_at": datetime.now(timezone.utc).isoformat(),
    }


def transform_hourly_weather(raw_data: Dict) -> List[Dict]:
    """
    Transform raw API response into hourly weather records.

    Args:
        raw_data: Raw API response dictionary

    Returns:
        List of hourly weather records
    """
    hourly = raw_data.get("hourly", {})
    location_name = raw_data.get("location_name")

    times = hourly.get("time", [])
    temperatures = hourly.get("temperature_2m", [])
    humidity = hourly.get("relative_humidity_2m", [])
    wind_speeds = hourly.get("wind_speed_10m", [])

    records = []
    for i in range(len(times)):
        records.append(
            {
                "location_name": location_name,
                "forecast_time": times[i],
                "temperature_celsius": temperatures[i]
                if i < len(temperatures)
                else None,
                "relative_humidity_percent": humidity[i] if i < len(humidity) else None,
                "wind_speed_kmh": wind_speeds[i] if i < len(wind_speeds) else None,
                "loaded_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    return records


def transform_all_data(raw_data_list: List[Dict]) -> Dict[str, pd.DataFrame]:
    """
    Tranform all raw data into structured DataFrames.

    Args:
        raw_data_list: List of raw API responses

    Returns:
        Dictionary with 'current' and 'hourly' DataFrames
    """
    current_records = []
    hourly_records = []

    for raw_data in raw_data_list:
        current_records.append(transform_current_weather(raw_data))
        hourly_records.extend(transform_hourly_weather(raw_data))

    current_df = pd.DataFrame(current_records)
    hourly_df = pd.DataFrame(hourly_records)

    current_df = current_df.drop_duplicates(
        subset=["location_name", "observation_time"]
    )
    hourly_df = hourly_df.drop_duplicates(subset=["location_name", "forecast_time"])

    logger.info(f"Transformed {len(current_df)} current weather records")
    logger.info(f"Transformed {len(hourly_df)} hourly weather records")

    return {"current": current_df, "hourly": hourly_df}
