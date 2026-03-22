"""Fetch actual weather outcome data from Open-Meteo."""

import re
import logging
import requests
from datetime import datetime, timezone, timedelta

import config

logger = logging.getLogger(__name__)

# City coordinates for common weather market locations
CITY_COORDS = {
    "new york": (40.7128, -74.0060),
    "nyc": (40.7128, -74.0060),
    "los angeles": (34.0522, -118.2437),
    "la": (34.0522, -118.2437),
    "chicago": (41.8781, -87.6298),
    "miami": (25.7617, -80.1918),
    "houston": (29.7604, -95.3698),
    "phoenix": (33.4484, -112.0740),
    "dallas": (32.7767, -96.7970),
    "denver": (39.7392, -104.9903),
    "seattle": (47.6062, -122.3321),
    "atlanta": (33.7490, -84.3880),
    "boston": (42.3601, -71.0589),
    "san francisco": (37.7749, -122.4194),
    "washington": (38.9072, -77.0369),
    "dc": (38.9072, -77.0369),
    "london": (51.5074, -0.1278),
    "paris": (48.8566, 2.3522),
    "tokyo": (35.6762, 139.6503),
}


def extract_city(text: str) -> tuple[str, tuple[float, float]] | None:
    """Try to extract a city name and coordinates from market text."""
    lower = text.lower()
    for city, coords in CITY_COORDS.items():
        if city in lower:
            return city, coords
    return None


def extract_date_from_text(text: str) -> str | None:
    """Try to extract a date reference from market text."""
    # Match patterns like "March 15", "January 2026", "2026-03-15"
    patterns = [
        r'(\d{4}-\d{2}-\d{2})',
        r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:,?\s+\d{4})?)',
    ]
    for pat in patterns:
        match = re.search(pat, text, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def fetch_weather_actual(lat: float, lon: float, date: str) -> dict | None:
    """
    Fetch actual weather data for a location and date from Open-Meteo.

    Args:
        lat: Latitude
        lon: Longitude
        date: Date string in YYYY-MM-DD format

    Returns:
        Dict with weather data or None on failure.
    """
    try:
        target_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        logger.warning("Invalid date format: %s", date)
        return None

    today = datetime.now(timezone.utc).date()

    if target_date > today:
        logger.info("Date %s is in the future, no actual data available", date)
        return None

    # Use archive API for past dates
    if target_date < today - timedelta(days=5):
        base_url = config.OPEN_METEO_ARCHIVE_URL
    else:
        base_url = config.OPEN_METEO_BASE_URL

    try:
        resp = requests.get(
            f"{base_url}/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "start_date": date,
                "end_date": date,
                "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum,wind_speed_10m_max",
                "temperature_unit": "fahrenheit",
                "timezone": "auto",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.warning("Open-Meteo fetch failed for %s on %s: %s", (lat, lon), date, e)
        return None

    daily = data.get("daily", {})
    if not daily or not daily.get("time"):
        return None

    return {
        "date": date,
        "latitude": lat,
        "longitude": lon,
        "temp_max_f": _first(daily.get("temperature_2m_max")),
        "temp_min_f": _first(daily.get("temperature_2m_min")),
        "precipitation_mm": _first(daily.get("precipitation_sum")),
        "snowfall_cm": _first(daily.get("snowfall_sum")),
        "wind_max_mph": _first(daily.get("wind_speed_10m_max")),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def _first(lst):
    """Get first element of list or None."""
    if lst and len(lst) > 0:
        return lst[0]
    return None


def fetch_outcomes_for_markets(markets_df) -> list[dict]:
    """
    For resolved markets, attempt to fetch actual weather outcomes.

    Returns list of outcome records.
    """
    import pandas as pd

    outcomes = []
    if markets_df.empty:
        return outcomes

    resolved = markets_df[markets_df["resolved"] == True]
    if resolved.empty:
        logger.info("No resolved markets to fetch outcomes for")
        return outcomes

    for _, row in resolved.iterrows():
        text = f"{row.get('question', '')} {row.get('description', '')}"

        city_info = extract_city(text)
        if not city_info:
            continue

        city_name, (lat, lon) = city_info
        date_str = extract_date_from_text(text)
        if not date_str:
            # Try using end_date
            end_date = row.get("end_date", "")
            if end_date:
                try:
                    date_str = end_date[:10]  # Take YYYY-MM-DD part
                except (IndexError, TypeError):
                    continue

        if not date_str:
            continue

        # Normalize date format
        try:
            for fmt in ["%Y-%m-%d", "%B %d, %Y", "%B %d %Y", "%B %Y"]:
                try:
                    parsed = datetime.strptime(date_str, fmt)
                    date_str = parsed.strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue
        except Exception:
            continue

        weather = fetch_weather_actual(lat, lon, date_str)
        if weather:
            weather["market_id"] = row["market_id"]
            weather["source"] = row["source"]
            weather["city"] = city_name
            weather["market_resolution"] = row.get("resolution", "")
            outcomes.append(weather)

    logger.info("Fetched %d weather outcomes for resolved markets", len(outcomes))
    return outcomes
