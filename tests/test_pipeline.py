import pytest
from src.extract import extract_weather_data
from src.transform import transform_current_weather
from config.config import API_BASE_URL


class TestExtract:
    def test_extract_weather_data_success(self):
        result = extract_weather_data(
            base_url=API_BASE_URL,
            latitude=51.5074,
            longitude=0.1278,
            location_name="London",
        )

        assert result is not None
        assert "current_weather" in result
        assert result["location_name"] == "London"

    def test_extract_weather_data_invalid_coordinates(self):
        result = extract_weather_data(
            base_url=API_BASE_URL,
            latitude=9999,
            longitude=9999,
            location_name="Invalid",
        )

        assert result is None or "error" in result


class TestTransform:
    def test_transform_current_weather(self):
        raw_data = {
            "location_name": "London",
            "latitude": 51.5074,
            "longitude": 0.1278,
            "current_weather": {
                "temperature": 15.5,
                "windspeed": 10.2,
                "winddirection": 180,
                "weathercode": 1,
                "time": "2025-01-15T12:00",
            },
            "extracted_at": "2025-01-15T12:00:00",
        }

        result = transform_current_weather(raw_data)

        assert result["location_name"] == "London"
        assert result["temperature_celsius"] == 15.5
        assert result["wind_speed_kmh"] == 10.2

    def test_transform_handles_missing_data(self):
        raw_data = {"location_name": "Unknown", "current_weather": {}}

        result = transform_current_weather(raw_data)

        assert result["location_name"] == "Unknown"
        assert result["temperature_celsius"] is None
