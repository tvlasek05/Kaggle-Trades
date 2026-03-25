"""Fetch actual weather outcomes from Open-Meteo for market verification."""

import os
import re
import time
from datetime import datetime, timezone

import requests
import pandas as pd

import config


# City coordinates for common prediction market locations
CITY_COORDS = {
    "new york": (40.7128, -74.0060),
    "nyc": (40.7128, -74.0060),
    "los angeles": (34.0522, -118.2437),
    "la": (34.0522, -118.2437),
    "chicago": (41.8781, -87.6298),
    "houston": (29.7604, -95.3698),
    "miami": (25.7617, -80.1918),
    "denver": (39.7392, -104.9903),
    "phoenix": (33.4484, -112.0740),
    "seattle": (47.6062, -122.3321),
    "atlanta": (33.7490, -84.3880),
    "boston": (42.3601, -71.0589),
    "dallas": (32.7767, -96.7970),
    "san francisco": (37.7749, -122.4194),
    "sf": (37.7749, -122.4194),
    "washington": (38.9072, -77.0369),
    "dc": (38.9072, -77.0369),
    "las vegas": (36.1699, -115.1398),
    "minneapolis": (44.9778, -93.2650),
    "detroit": (42.3314, -83.0458),
    "philadelphia": (39.9526, -75.1652),
    "portland": (45.5152, -122.6784),
}


def extract_city_from_text(text):
    """Try to extract a city name from market question/description."""
    text_lower = text.lower()
    for city, coords in CITY_COORDS.items():
        if city in text_lower:
            return city, coords
    return None, None


def extract_date_from_text(text):
    """Try to extract a target date from market text."""
    # Common patterns: "on March 25", "by March 25, 2026", "March 2026"
    patterns = [
        r"(\w+ \d{1,2},?\s*\d{4})",
        r"(\d{4}-\d{2}-\d{2})",
        r"(\d{1,2}/\d{1,2}/\d{4})",
    ]
    for pat in patterns:
        match = re.search(pat, text)
        if match:
            date_str = match.group(1)
            for fmt in ["%B %d, %Y", "%B %d %Y", "%Y-%m-%d", "%m/%d/%Y"]:
                try:
                    return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
                except ValueError:
                    continue
    return None


def extract_temperature_threshold(text):
    """Try to extract a temperature threshold from market text."""
    # Patterns like "above 80°F", "reach 90", "exceed 100°F"
    patterns = [
        r"(\d+)\s*°?\s*[Ff]",
        r"above\s+(\d+)",
        r"exceed\s+(\d+)",
        r"reach\s+(\d+)",
        r"over\s+(\d+)",
        r"below\s+(\d+)",
        r"under\s+(\d+)",
    ]
    for pat in patterns:
        match = re.search(pat, text)
        if match:
            return float(match.group(1))
    return None


def fetch_historical_weather(lat, lon, start_date, end_date, variables=None):
    """Fetch historical weather data from Open-Meteo Archive API."""
    if variables is None:
        variables = [
            "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
            "precipitation_sum", "snowfall_sum", "windspeed_10m_max",
        ]

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(variables),
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York",
    }

    try:
        resp = requests.get(
            config.OPEN_METEO_ARCHIVE_URL,
            params=params,
            timeout=config.REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if "daily" in data:
            daily = data["daily"]
            df = pd.DataFrame(daily)
            return df
    except Exception as e:
        print(f"  [WARN] Open-Meteo archive request failed: {e}")

    return None


def fetch_forecast(lat, lon, variables=None):
    """Fetch current forecast from Open-Meteo."""
    if variables is None:
        variables = [
            "temperature_2m_max", "temperature_2m_min",
            "precipitation_sum", "snowfall_sum",
        ]

    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": ",".join(variables),
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York",
        "forecast_days": 16,
    }

    try:
        resp = requests.get(
            config.OPEN_METEO_FORECAST_URL,
            params=params,
            timeout=config.REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if "daily" in data:
            return pd.DataFrame(data["daily"])
    except Exception as e:
        print(f"  [WARN] Open-Meteo forecast request failed: {e}")

    return None


def verify_resolved_markets(markets_df):
    """For resolved markets, attempt to verify outcomes against actual weather.

    Returns a DataFrame of verification results.
    """
    results = []
    resolved = markets_df[markets_df["resolved"] == True]

    if resolved.empty:
        print("[Weather] No resolved markets to verify")
        return pd.DataFrame()

    print(f"[Weather] Verifying {len(resolved)} resolved markets...")

    for _, market in resolved.iterrows():
        text = f"{market.get('question', '')} {market.get('description', '')}"
        city, coords = extract_city_from_text(text)
        target_date = extract_date_from_text(text)
        threshold = extract_temperature_threshold(text)

        if city and coords and target_date:
            time.sleep(config.REQUEST_DELAY)
            weather = fetch_historical_weather(
                coords[0], coords[1], target_date, target_date
            )
            if weather is not None and not weather.empty:
                actual_high = weather.get("temperature_2m_max", [None])[0]
                actual_low = weather.get("temperature_2m_min", [None])[0]
                actual_precip = weather.get("precipitation_sum", [None])[0]

                results.append({
                    "source": market["source"],
                    "market_id": market["market_id"],
                    "question": market["question"],
                    "city": city,
                    "target_date": target_date,
                    "threshold_f": threshold,
                    "actual_high_f": actual_high,
                    "actual_low_f": actual_low,
                    "actual_precip_in": actual_precip,
                    "market_outcome": market.get("outcome"),
                    "implied_prob": market.get("implied_prob"),
                    "verified_at": datetime.now(timezone.utc).isoformat(),
                })

    if results:
        df = pd.DataFrame(results)
        # Save outcomes
        if os.path.exists(config.OUTCOMES_FILE):
            existing = pd.read_csv(config.OUTCOMES_FILE)
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(subset=["source", "market_id"], keep="last")
        os.makedirs(os.path.dirname(config.OUTCOMES_FILE), exist_ok=True)
        df.to_csv(config.OUTCOMES_FILE, index=False)
        print(f"  Saved {len(results)} verification results")
        return df

    return pd.DataFrame()


def get_current_conditions_for_markets(markets_df):
    """Fetch current forecasts for active markets to enable comparison."""
    active = markets_df[markets_df["resolved"] == False]
    if active.empty:
        return {}

    forecasts = {}
    cities_fetched = set()

    for _, market in active.iterrows():
        text = f"{market.get('question', '')} {market.get('description', '')}"
        city, coords = extract_city_from_text(text)

        if city and coords and city not in cities_fetched:
            time.sleep(config.REQUEST_DELAY)
            forecast = fetch_forecast(coords[0], coords[1])
            if forecast is not None:
                forecasts[city] = forecast
                cities_fetched.add(city)

    return forecasts
