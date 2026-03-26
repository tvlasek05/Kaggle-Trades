"""Fetch actual weather outcomes from Open-Meteo for resolved markets."""

import csv
import time
from datetime import datetime, timedelta, timezone

import requests

import config


def fetch_daily_weather(city_key: str, date: str, variable: str) -> float | None:
    """Fetch a single daily weather value for a city on a given date.

    Args:
        city_key: Key into config.CITY_COORDS (e.g., "NY")
        date: Target date as "YYYY-MM-DD"
        variable: Open-Meteo daily variable name (e.g., "temperature_2m_max")

    Returns:
        The weather value or None on failure.
    """
    coords = config.CITY_COORDS.get(city_key)
    if not coords:
        return None

    params = {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "start_date": date,
        "end_date": date,
        "daily": variable,
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
        "timezone": coords["tz"],
    }

    try:
        resp = requests.get(config.OPEN_METEO_ARCHIVE_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        values = data.get("daily", {}).get(variable, [])
        if values and values[0] is not None:
            return float(values[0])
    except (requests.RequestException, ValueError, KeyError) as e:
        print(f"  Error fetching weather for {city_key} on {date}: {e}")
    return None


def load_existing_outcomes() -> dict[str, dict]:
    """Load existing outcome records keyed by (event_ticker, target_date)."""
    existing = {}
    if config.OUTCOMES_CSV.exists():
        with open(config.OUTCOMES_CSV, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = f"{row['event_ticker']}_{row['target_date']}"
                existing[key] = row
    return existing


def save_outcomes(outcomes: dict[str, dict]):
    """Save outcome records to CSV."""
    if not outcomes:
        return
    fieldnames = [
        "event_ticker", "series_ticker", "target_date", "city",
        "weather_variable", "actual_value", "fetched_at",
    ]
    with open(config.OUTCOMES_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in sorted(outcomes.values(), key=lambda r: r.get("target_date", "")):
            writer.writerow(row)


def run(markets: dict[str, dict] | None = None):
    """Fetch actual weather for resolved/past markets.

    Only fetches for dates that have already passed (so actual data exists).
    """
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    cutoff = (now - timedelta(days=5)).strftime("%Y-%m-%d")  # data available ~5 days back

    # Load markets if not provided
    if markets is None:
        markets = {}
        if config.MARKETS_CSV.exists():
            with open(config.MARKETS_CSV, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    markets[row["ticker"]] = row

    # Collect unique (event, date, series) combos that need weather
    existing_outcomes = load_existing_outcomes()
    to_fetch = {}

    for mkt in markets.values():
        target_date = mkt.get("target_date", "")
        series = mkt.get("series_ticker", "")
        event = mkt.get("event_ticker", "")
        city = config.SERIES_CITY_MAP.get(series, "")
        variable = config.SERIES_VARIABLE_MAP.get(series, "")

        if not target_date or not city or not variable:
            continue
        if target_date >= cutoff:
            continue  # data not yet available

        key = f"{event}_{target_date}"
        if key in existing_outcomes:
            continue  # already fetched

        to_fetch[key] = {
            "event_ticker": event,
            "series_ticker": series,
            "target_date": target_date,
            "city": city,
            "weather_variable": variable,
        }

    print(f"Weather outcomes to fetch: {len(to_fetch)}")

    fetched = 0
    for key, info in to_fetch.items():
        actual = fetch_daily_weather(info["city"], info["target_date"], info["weather_variable"])
        if actual is not None:
            info["actual_value"] = actual
            info["fetched_at"] = now.isoformat()
            existing_outcomes[key] = info
            fetched += 1
        time.sleep(0.2)  # respect rate limits

    save_outcomes(existing_outcomes)
    print(f"Weather outcomes: {fetched} new, {len(existing_outcomes)} total")
    return existing_outcomes


if __name__ == "__main__":
    run()
