"""Fetch weather prediction market data from Kalshi."""

import csv
import re
import time
from datetime import datetime, timezone

import requests

import config


def _request_with_retry(url: str, params: dict, max_retries: int = 3) -> requests.Response:
    """Make a GET request with exponential backoff retries."""
    for attempt in range(max_retries):
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 429:
            wait = 2 ** (attempt + 1)
            print(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()
    return resp


def fetch_events_for_series(series_ticker: str) -> list[dict]:
    """Fetch all events for a given series ticker from Kalshi."""
    events = []
    cursor = None
    url = f"{config.KALSHI_BASE_URL}/events"

    while True:
        params = {
            "series_ticker": series_ticker,
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor

        try:
            resp = _request_with_retry(url, params)
            data = resp.json()
        except requests.RequestException as e:
            print(f"  Error fetching events for {series_ticker}: {e}")
            break

        events.extend(data.get("events", []))
        cursor = data.get("cursor")
        if not cursor or not data.get("events"):
            break
        time.sleep(0.15)

    return events


def fetch_markets_for_event(event_ticker: str) -> list[dict]:
    """Fetch all markets for a given event ticker."""
    markets = []
    cursor = None
    url = f"{config.KALSHI_BASE_URL}/markets"

    while True:
        params = {
            "event_ticker": event_ticker,
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor

        try:
            resp = _request_with_retry(url, params)
            data = resp.json()
        except requests.RequestException as e:
            print(f"  Error fetching markets for {event_ticker}: {e}")
            break

        markets.extend(data.get("markets", []))
        cursor = data.get("cursor")
        if not cursor or not data.get("markets"):
            break
        time.sleep(0.15)

    return markets


def parse_strike_from_ticker(ticker: str) -> float | None:
    """Extract numeric strike value from a market ticker like HIGHNY-26MAR28-T55."""
    match = re.search(r"-T(\d+\.?\d*)$", ticker)
    if match:
        return float(match.group(1))
    match = re.search(r"-B(\d+\.?\d*)$", ticker)
    if match:
        return float(match.group(1))
    return None


def parse_date_from_event(event: dict) -> str | None:
    """Extract the target date from event data."""
    strike_date = event.get("strike_date") or event.get("close_time")
    if strike_date:
        try:
            dt = datetime.fromisoformat(strike_date.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Try parsing from event ticker like HIGHNY-26MAR28
    ticker = event.get("event_ticker", "")
    match = re.search(r"-(\d{2})([A-Z]{3})(\d{2})$", ticker)
    if match:
        day, mon_str, yr = match.groups()
        months = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
            "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
        }
        month = months.get(mon_str)
        if month:
            year = 2000 + int(yr)
            return f"{year}-{month:02d}-{int(day):02d}"
    return None


def load_existing_markets() -> dict[str, dict]:
    """Load existing market records keyed by ticker."""
    existing = {}
    if config.MARKETS_CSV.exists():
        with open(config.MARKETS_CSV, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing[row["ticker"]] = row
    return existing


def save_markets(markets: dict[str, dict]):
    """Save market records to CSV."""
    if not markets:
        return
    fieldnames = [
        "ticker", "event_ticker", "series_ticker", "title",
        "strike_value", "target_date", "city",
        "yes_bid", "yes_ask", "last_price", "volume", "open_interest",
        "status", "result", "close_time",
        "first_seen", "last_updated",
    ]
    with open(config.MARKETS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in sorted(markets.values(), key=lambda r: r.get("ticker", "")):
            writer.writerow(row)


def append_price_snapshot(records: list[dict]):
    """Append price snapshots to the rolling prices CSV."""
    if not records:
        return
    fieldnames = [
        "timestamp", "ticker", "yes_bid", "yes_ask", "last_price",
        "volume", "open_interest",
    ]
    file_exists = config.PRICES_CSV.exists()
    with open(config.PRICES_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        for r in records:
            writer.writerow(r)


def run():
    """Main fetch routine: pull all weather markets and record prices."""
    now = datetime.now(timezone.utc).isoformat()
    existing = load_existing_markets()
    price_snapshots = []
    new_count = 0
    updated_count = 0

    for series in config.KALSHI_WEATHER_SERIES:
        print(f"Fetching series: {series}")
        events = fetch_events_for_series(series)
        print(f"  Found {len(events)} events")

        for event in events:
            event_ticker = event.get("event_ticker", "")
            target_date = parse_date_from_event(event)
            markets = fetch_markets_for_event(event_ticker)

            for mkt in markets:
                ticker = mkt.get("ticker", "")
                strike = parse_strike_from_ticker(ticker)
                city = config.SERIES_CITY_MAP.get(series, "")

                record = {
                    "ticker": ticker,
                    "event_ticker": event_ticker,
                    "series_ticker": series,
                    "title": mkt.get("subtitle") or mkt.get("title") or event.get("title", ""),
                    "strike_value": strike,
                    "target_date": target_date,
                    "city": city,
                    "yes_bid": mkt.get("yes_bid", ""),
                    "yes_ask": mkt.get("yes_ask", ""),
                    "last_price": mkt.get("last_price", ""),
                    "volume": mkt.get("volume", ""),
                    "open_interest": mkt.get("open_interest", ""),
                    "status": mkt.get("status", ""),
                    "result": mkt.get("result", ""),
                    "close_time": mkt.get("close_time", ""),
                    "last_updated": now,
                }

                if ticker in existing:
                    record["first_seen"] = existing[ticker].get("first_seen", now)
                    updated_count += 1
                else:
                    record["first_seen"] = now
                    new_count += 1

                existing[ticker] = record

                # Price snapshot for time series
                price_snapshots.append({
                    "timestamp": now,
                    "ticker": ticker,
                    "yes_bid": mkt.get("yes_bid", ""),
                    "yes_ask": mkt.get("yes_ask", ""),
                    "last_price": mkt.get("last_price", ""),
                    "volume": mkt.get("volume", ""),
                    "open_interest": mkt.get("open_interest", ""),
                })

        time.sleep(0.5)  # respect rate limits between series

    save_markets(existing)
    append_price_snapshot(price_snapshots)
    print(f"\nMarkets: {new_count} new, {updated_count} updated, {len(existing)} total")
    print(f"Price snapshots: {len(price_snapshots)} recorded")
    return existing


if __name__ == "__main__":
    run()
