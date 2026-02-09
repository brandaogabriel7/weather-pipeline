import os

API_BASE_URL = "https://api.open-meteo.com/v1/forecast"
LOCATIONS = [
    {"name": "London", "lat": 51.5074, "lon": -0.1278},
    {"name": "New York", "lat": 40.7128, "lon": 139.6503},
    {"name": "Tokyo", "lat": 35.6762, "lon": 139.6503},
]

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "weather_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}
