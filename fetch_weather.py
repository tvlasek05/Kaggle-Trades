"""Fetch actual weather outcomes from Open-Meteo for market verification."""

import logging
from datetime import datetime, timedelta, timezone

import requests

import config

logger = logging.getLogger(__name__)


def fetch_forecast(city_code, days=7):
    """Fetch weather forecast for a city."""
    city = config.CITIES.get(city_code)
    if not city:
        logger.warning("Unknown city: %s", city_code)
        return None

    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum,weathercode,windspeed_10m_max",
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York",
        "forecast_days": days,
    }

    try:
        resp = requests.get(config.OPEN_METEO_FORECAST_URL, params=params, timeout=config.REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.json()
    except requests.RequestException as e:
        logger.warning("Forecast fetch error for %s: %s", city_code, e)
    return None


def fetch_historical(city_code, start_date, end_date):
    """Fetch historical weather data for verification of resolved markets."""
    city = config.CITIES.get(city_code)
    if not city:
        return None

    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum,weathercode,windspeed_10m_max",
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York",
        "start_date": start_date,
        "end_date": end_date,
    }

    try:
        resp = requests.get(config.OPEN_METEO_ARCHIVE_URL, params=params, timeout=config.REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.json()
    except requests.RequestException as e:
        logger.warning("Historical fetch error for %s: %s", city_code, e)
    return None


def get_weather_actuals_rows(city_code, data):
    """Convert Open-Meteo response to flat rows."""
    if not data or "daily" not in data:
        return []

    daily = data["daily"]
    times = daily.get("time", [])
    rows = []

    for i, date_str in enumerate(times):
        rows.append({
            "date": date_str,
            "city": city_code,
            "temp_max_f": daily.get("temperature_2m_max", [None])[i],
            "temp_min_f": daily.get("temperature_2m_min", [None])[i],
            "precipitation_mm": daily.get("precipitation_sum", [None])[i],
            "snowfall_mm": daily.get("snowfall_sum", [None])[i],
            "weathercode": daily.get("weathercode", [None])[i],
            "windspeed_max_kmh": daily.get("windspeed_10m_max", [None])[i],
        })

    return rows


def fetch_all_weather_actuals():
    """Fetch forecast + recent historical data for all tracked cities."""
    all_rows = []
    today = datetime.now(timezone.utc).date()

    for city_code in config.CITIES:
        # Fetch forecast (next 7 days)
        forecast_data = fetch_forecast(city_code, days=7)
        if forecast_data:
            all_rows.extend(get_weather_actuals_rows(city_code, forecast_data))

        # Fetch last 7 days of historical actuals
        start = (today - timedelta(days=7)).isoformat()
        end = (today - timedelta(days=1)).isoformat()
        hist_data = fetch_historical(city_code, start, end)
        if hist_data:
            all_rows.extend(get_weather_actuals_rows(city_code, hist_data))

    logger.info("Weather actuals: %d rows fetched", len(all_rows))
    return all_rows
