"""
Fetch actual weather outcomes from Open-Meteo (free, no API key required).

Open-Meteo provides:
- Historical weather: https://archive-api.open-meteo.com/v1/archive
- Forecast: https://api.open-meteo.com/v1/forecast

Used to verify prediction market outcomes.
"""

import requests
from datetime import datetime, timedelta, timezone


ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

# Major US cities commonly used in weather prediction markets
CITIES = {
    "new_york": {"lat": 40.7128, "lon": -74.0060, "name": "New York, NY"},
    "chicago": {"lat": 41.8781, "lon": -87.6298, "name": "Chicago, IL"},
    "los_angeles": {"lat": 34.0522, "lon": -118.2437, "name": "Los Angeles, CA"},
    "miami": {"lat": 25.7617, "lon": -80.1918, "name": "Miami, FL"},
    "houston": {"lat": 29.7604, "lon": -95.3698, "name": "Houston, TX"},
    "phoenix": {"lat": 33.4484, "lon": -112.0740, "name": "Phoenix, AZ"},
    "denver": {"lat": 39.7392, "lon": -104.9903, "name": "Denver, CO"},
    "seattle": {"lat": 47.6062, "lon": -122.3321, "name": "Seattle, WA"},
    "atlanta": {"lat": 33.7490, "lon": -84.3880, "name": "Atlanta, GA"},
    "boston": {"lat": 42.3601, "lon": -71.0589, "name": "Boston, MA"},
}

WEATHER_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "snowfall_sum",
    "windspeed_10m_max",
    "rain_sum",
]


def fetch_historical_weather(city_key, start_date, end_date):
    """Fetch historical weather data for a city from Open-Meteo Archive API."""
    city = CITIES.get(city_key)
    if not city:
        return None

    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(WEATHER_VARS),
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
        "windspeed_unit": "mph",
        "timezone": "America/New_York",
    }

    try:
        resp = requests.get(ARCHIVE_URL, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            daily = data.get("daily", {})
            dates = daily.get("time", [])
            records = []
            for i, date in enumerate(dates):
                record = {
                    "city": city_key,
                    "city_name": city["name"],
                    "date": date,
                    "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
                }
                for var in WEATHER_VARS:
                    vals = daily.get(var, [])
                    record[var] = vals[i] if i < len(vals) else None
                records.append(record)
            return records
        else:
            print(f"  Open-Meteo archive error {resp.status_code}: {resp.text[:200]}")
    except requests.RequestException as e:
        print(f"  Open-Meteo archive request failed: {e}")

    return None


def fetch_forecast_weather(city_key, days=7):
    """Fetch weather forecast for a city from Open-Meteo Forecast API."""
    city = CITIES.get(city_key)
    if not city:
        return None

    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "daily": ",".join(WEATHER_VARS),
        "forecast_days": days,
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
        "windspeed_unit": "mph",
        "timezone": "America/New_York",
    }

    try:
        resp = requests.get(FORECAST_URL, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            daily = data.get("daily", {})
            dates = daily.get("time", [])
            records = []
            for i, date in enumerate(dates):
                record = {
                    "city": city_key,
                    "city_name": city["name"],
                    "date": date,
                    "is_forecast": True,
                    "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
                }
                for var in WEATHER_VARS:
                    vals = daily.get(var, [])
                    record[var] = vals[i] if i < len(vals) else None
                records.append(record)
            return records
    except requests.RequestException as e:
        print(f"  Open-Meteo forecast request failed: {e}")

    return None


def fetch_all_city_weather(lookback_days=30):
    """Fetch historical + forecast weather for all tracked cities."""
    all_records = []
    end_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    for city_key in CITIES:
        print(f"  Fetching weather for {CITIES[city_key]['name']}...")

        # Historical data
        historical = fetch_historical_weather(city_key, start_date, end_date)
        if historical:
            all_records.extend(historical)

        # Forecast data
        forecast = fetch_forecast_weather(city_key, days=7)
        if forecast:
            all_records.extend(forecast)

    print(f"  Total weather records: {len(all_records)}")
    return all_records


if __name__ == "__main__":
    import json
    records = fetch_all_city_weather(lookback_days=7)
    for r in records[:3]:
        print(json.dumps(r, indent=2))
