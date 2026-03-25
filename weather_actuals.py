"""
Fetch actual weather outcomes from Open-Meteo API for verifying
prediction market resolutions.

Open-Meteo is free, no API key required.
"""

import json
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
ACTUALS_FILE = os.path.join(DATA_DIR, "weather_actuals.csv")

# Major US cities with their coordinates (lat, lon)
CITY_COORDS = {
    "new_york": (40.7128, -74.0060),
    "los_angeles": (34.0522, -118.2437),
    "chicago": (41.8781, -87.6298),
    "miami": (25.7617, -80.1918),
    "houston": (29.7604, -95.3698),
    "phoenix": (33.4484, -112.0740),
    "boston": (42.3601, -71.0589),
    "denver": (39.7392, -104.9903),
    "seattle": (47.6062, -122.3321),
    "atlanta": (33.7490, -84.3880),
    "dallas": (32.7767, -96.7970),
    "san_francisco": (37.7749, -122.4194),
    "washington_dc": (38.9072, -77.0369),
    "minneapolis": (44.9778, -93.2650),
}

OPEN_METEO_BASE = "https://api.open-meteo.com/v1"


def fetch_city_weather(city, lat, lon, start_date, end_date):
    """Fetch historical daily weather for a city from Open-Meteo."""
    try:
        resp = requests.get(
            f"{OPEN_METEO_BASE}/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": ",".join([
                    "temperature_2m_max",
                    "temperature_2m_min",
                    "temperature_2m_mean",
                    "precipitation_sum",
                    "snowfall_sum",
                    "rain_sum",
                    "wind_speed_10m_max",
                    "weather_code",
                ]),
                "temperature_unit": "fahrenheit",
                "precipitation_unit": "inch",
                "wind_speed_unit": "mph",
                "start_date": start_date,
                "end_date": end_date,
                "timezone": "America/New_York",
            },
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            daily = data.get("daily", {})
            dates = daily.get("time", [])
            rows = []
            for i, date in enumerate(dates):
                rows.append({
                    "date": date,
                    "city": city,
                    "lat": lat,
                    "lon": lon,
                    "temp_max_f": _safe_get(daily, "temperature_2m_max", i),
                    "temp_min_f": _safe_get(daily, "temperature_2m_min", i),
                    "temp_mean_f": _safe_get(daily, "temperature_2m_mean", i),
                    "precip_inches": _safe_get(daily, "precipitation_sum", i),
                    "snow_inches": _safe_get(daily, "snowfall_sum", i),
                    "rain_inches": _safe_get(daily, "rain_sum", i),
                    "wind_max_mph": _safe_get(daily, "wind_speed_10m_max", i),
                    "weather_code": _safe_get(daily, "weather_code", i),
                })
            return rows
        else:
            print(f"  [Open-Meteo] HTTP {resp.status_code} for {city}")
    except Exception as e:
        print(f"  [Open-Meteo] Error for {city}: {e}")
    return []


def _safe_get(daily_dict, key, index):
    """Safely get a value from Open-Meteo daily arrays."""
    values = daily_dict.get(key, [])
    if index < len(values):
        return values[index]
    return None


def fetch_recent_weather(days_back=7):
    """Fetch recent weather for all tracked cities."""
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")

    all_rows = []
    print(f"Fetching weather actuals ({start_date} to {end_date})...")
    for city, (lat, lon) in CITY_COORDS.items():
        rows = fetch_city_weather(city, lat, lon, start_date, end_date)
        all_rows.extend(rows)
        print(f"  {city}: {len(rows)} daily records")

    return all_rows


def load_existing_actuals():
    """Load existing weather actuals from CSV."""
    if os.path.exists(ACTUALS_FILE):
        return pd.read_csv(ACTUALS_FILE)
    return pd.DataFrame()


def save_actuals(df):
    """Save weather actuals to CSV."""
    os.makedirs(DATA_DIR, exist_ok=True)
    df.to_csv(ACTUALS_FILE, index=False)


def update_weather_actuals(days_back=7):
    """Fetch and merge new weather actuals into the persistent dataset."""
    new_rows = fetch_recent_weather(days_back)
    if not new_rows:
        print("No new weather data fetched.")
        return 0

    new_df = pd.DataFrame(new_rows)
    existing = load_existing_actuals()

    if not existing.empty:
        combined = pd.concat([existing, new_df], ignore_index=True)
        # Deduplicate on (date, city)
        combined = combined.drop_duplicates(subset=["date", "city"], keep="last")
        combined = combined.sort_values(["date", "city"]).reset_index(drop=True)
    else:
        combined = new_df

    save_actuals(combined)
    return len(new_rows)


def match_market_to_actuals(question, actuals_df):
    """
    Try to match a market question to actual weather data.
    Returns relevant actuals or None.
    """
    q = question.lower()

    # Try to identify city
    matched_city = None
    city_aliases = {
        "new york": "new_york", "nyc": "new_york", "ny": "new_york",
        "los angeles": "los_angeles", "la": "los_angeles",
        "chicago": "chicago", "chi": "chicago",
        "miami": "miami", "houston": "houston",
        "phoenix": "phoenix", "boston": "boston",
        "denver": "denver", "seattle": "seattle",
        "atlanta": "atlanta", "dallas": "dallas",
        "san francisco": "san_francisco", "sf": "san_francisco",
        "washington": "washington_dc", "dc": "washington_dc",
        "minneapolis": "minneapolis",
    }
    for alias, city in city_aliases.items():
        if alias in q:
            matched_city = city
            break

    if matched_city is None or actuals_df.empty:
        return None

    city_data = actuals_df[actuals_df["city"] == matched_city]
    if city_data.empty:
        return None

    return city_data


if __name__ == "__main__":
    n = update_weather_actuals(days_back=14)
    print(f"\nUpdated {n} weather records")
