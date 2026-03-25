"""Fetch actual weather outcomes from Open-Meteo for resolved market verification."""

import re
import pandas as pd
from datetime import datetime, timezone

import config
from fetch_markets import _request_with_retry


# City coordinates for common prediction market locations
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
    "boston": (42.3601, -71.0589),
    "atlanta": (33.7490, -84.3880),
    "san francisco": (37.7749, -122.4194),
    "sf": (37.7749, -122.4194),
    "washington": (38.9072, -77.0369),
    "dc": (38.9072, -77.0369),
    "london": (51.5074, -0.1278),
    "paris": (48.8566, 2.3522),
    "tokyo": (35.6762, 139.6503),
}


def extract_city_from_title(title):
    """Try to extract a city name from a market title."""
    title_lower = title.lower()
    for city, coords in CITY_COORDS.items():
        if city in title_lower:
            return city, coords
    return None, None


def extract_date_from_title(title):
    """Try to extract a date reference from a market title."""
    # Look for patterns like "March 2026", "Jan 15", "2026-03-25", etc.
    patterns = [
        r"(\d{4}-\d{2}-\d{2})",
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s*(\d{4})?",
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),?\s*(\d{4})?",
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})",
    ]
    for p in patterns:
        m = re.search(p, title, re.IGNORECASE)
        if m:
            return m.group(0)
    return None


def fetch_historical_weather(lat, lon, start_date, end_date, variables=None):
    """Fetch historical weather data from Open-Meteo Archive API.

    Args:
        lat, lon: Coordinates
        start_date, end_date: YYYY-MM-DD strings
        variables: List of weather variables (default: common set)

    Returns:
        dict with daily weather data or None
    """
    if variables is None:
        variables = [
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "precipitation_sum",
            "snowfall_sum",
            "wind_speed_10m_max",
        ]

    data = _request_with_retry(
        config.OPEN_METEO_HISTORICAL,
        params={
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "daily": ",".join(variables),
            "temperature_unit": "fahrenheit",
            "timezone": "America/New_York",
        },
    )
    return data


def fetch_weather_for_resolved_markets(markets_df):
    """For resolved markets, try to fetch actual weather outcomes.

    Returns a DataFrame of weather actuals matched to markets.
    """
    resolved = markets_df[markets_df["resolved"] == True]
    if resolved.empty:
        print("No resolved markets to check weather for.")
        return pd.DataFrame()

    records = []
    for _, market in resolved.iterrows():
        title = str(market.get("title", ""))
        city, coords = extract_city_from_title(title)
        if not coords:
            continue

        date_ref = extract_date_from_title(title)
        if not date_ref:
            # Use end_date from market as fallback
            end = str(market.get("end_date", ""))
            if end:
                date_ref = end[:10]  # Take YYYY-MM-DD portion
            else:
                continue

        # Try to parse and fetch a small date window
        try:
            # Use the date reference as a rough target
            weather = fetch_historical_weather(
                coords[0], coords[1],
                date_ref[:10] if len(date_ref) >= 10 else date_ref,
                date_ref[:10] if len(date_ref) >= 10 else date_ref,
            )
        except Exception as e:
            print(f"  [WARN] Weather fetch failed for {title}: {e}")
            continue

        if weather and "daily" in weather:
            daily = weather["daily"]
            for i, date in enumerate(daily.get("time", [])):
                records.append({
                    "market_id": market["market_id"],
                    "source": market["source"],
                    "title": title,
                    "city": city,
                    "date": date,
                    "temp_max_f": _safe_idx(daily.get("temperature_2m_max"), i),
                    "temp_min_f": _safe_idx(daily.get("temperature_2m_min"), i),
                    "temp_mean_f": _safe_idx(daily.get("temperature_2m_mean"), i),
                    "precip_mm": _safe_idx(daily.get("precipitation_sum"), i),
                    "snow_cm": _safe_idx(daily.get("snowfall_sum"), i),
                    "wind_max_kmh": _safe_idx(daily.get("wind_speed_10m_max"), i),
                    "market_resolution": market.get("resolution", ""),
                    "implied_prob": market.get("outcome_yes_price", 0),
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                })

    df = pd.DataFrame(records)
    if not df.empty:
        try:
            existing = pd.read_csv(config.WEATHER_ACTUALS_FILE)
            df = pd.concat([existing, df], ignore_index=True)
            df.drop_duplicates(subset=["market_id", "date"], keep="last", inplace=True)
        except FileNotFoundError:
            pass
        df.to_csv(config.WEATHER_ACTUALS_FILE, index=False)
        print(f"Saved {len(records)} weather actual records to {config.WEATHER_ACTUALS_FILE}")

    return df


def _safe_idx(lst, idx):
    """Safely index into a list."""
    try:
        return lst[idx] if lst else None
    except (IndexError, TypeError):
        return None


if __name__ == "__main__":
    try:
        markets_df = pd.read_csv(config.MARKETS_FILE)
        fetch_weather_for_resolved_markets(markets_df)
    except FileNotFoundError:
        print("No markets data found. Run fetch_markets.py first.")
