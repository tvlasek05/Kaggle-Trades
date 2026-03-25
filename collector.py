"""Collect weather prediction market data from Polymarket and Kalshi."""

import csv
import json
import os
import time
from datetime import datetime, timezone

import requests
import pandas as pd

import config


def _get(url, params=None, headers=None):
    """Make a GET request with retry logic."""
    for attempt in range(3):
        try:
            resp = requests.get(
                url, params=params, headers=headers, timeout=config.REQUEST_TIMEOUT
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == 2:
                print(f"  [WARN] Request failed after 3 attempts: {url} - {e}")
                return None
            time.sleep(1 * (attempt + 1))
    return None


def _is_weather_market(title, description=""):
    """Check if a market is weather-related based on keywords."""
    text = (title + " " + description).lower()
    return any(kw in text for kw in config.WEATHER_KEYWORDS)


# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------

def fetch_polymarket_weather_markets():
    """Fetch weather-related markets from Polymarket Gamma API."""
    print("[Polymarket] Fetching weather markets...")
    markets = []

    for keyword in ["weather", "temperature", "hurricane", "storm", "heat", "snow"]:
        time.sleep(config.REQUEST_DELAY)
        data = _get(
            f"{config.POLYMARKET_GAMMA_URL}/markets",
            params={"tag": keyword, "limit": 100, "active": "true"},
        )
        if data and isinstance(data, list):
            for m in data:
                markets.append(m)

    # Also search by text
    for keyword in ["temperature", "hurricane", "snow", "heatwave", "drought", "flood"]:
        time.sleep(config.REQUEST_DELAY)
        data = _get(
            f"{config.POLYMARKET_GAMMA_URL}/markets",
            params={"search": keyword, "limit": 50},
        )
        if data and isinstance(data, list):
            for m in data:
                markets.append(m)

    # Deduplicate by condition_id
    seen = set()
    unique = []
    for m in markets:
        cid = m.get("condition_id") or m.get("id", "")
        if cid and cid not in seen:
            seen.add(cid)
            if _is_weather_market(m.get("question", ""), m.get("description", "")):
                unique.append(m)

    print(f"  Found {len(unique)} weather-related markets on Polymarket")
    return unique


def parse_polymarket_markets(raw_markets):
    """Parse raw Polymarket market data into standardized rows."""
    rows = []
    now = datetime.now(timezone.utc).isoformat()

    for m in raw_markets:
        # Extract price from various possible fields
        price = None
        for field in ["outcomePrices", "bestAsk", "lastTradePrice"]:
            val = m.get(field)
            if val is not None:
                if isinstance(val, str):
                    try:
                        parsed = json.loads(val)
                        if isinstance(parsed, list) and len(parsed) > 0:
                            price = float(parsed[0])
                        else:
                            price = float(parsed)
                    except (json.JSONDecodeError, ValueError, TypeError):
                        pass
                elif isinstance(val, (int, float)):
                    price = float(val)
                if price is not None:
                    break

        resolved = m.get("closed", False) or m.get("resolved", False)
        outcome = None
        if resolved:
            outcome_val = m.get("outcome")
            if outcome_val is not None:
                outcome = str(outcome_val)

        rows.append({
            "source": "polymarket",
            "market_id": m.get("condition_id") or m.get("id", ""),
            "question": m.get("question", ""),
            "description": m.get("description", "")[:500],
            "end_date": m.get("end_date_iso") or m.get("endDate", ""),
            "resolved": resolved,
            "outcome": outcome,
            "volume": m.get("volume", 0),
            "last_price": price,
            "implied_prob": price,
            "fetched_at": now,
            "tags": m.get("tags", ""),
        })
    return rows


# ---------------------------------------------------------------------------
# Kalshi
# ---------------------------------------------------------------------------

def fetch_kalshi_weather_markets():
    """Fetch weather-related markets from Kalshi API."""
    print("[Kalshi] Fetching weather markets...")
    markets = []

    # Kalshi organizes by series/categories - try weather-related ones
    weather_series = [
        "KXHIGHNY", "KXHIGHLA", "KXHIGHCHI", "KXHIGHHOU", "KXHIGHMIA",
        "KXHIGHDEN", "KXHIGHPHX", "KXHIGHSEA", "KXHIGHATL", "KXHIGHBOS",
        "KXLOWNY", "KXLOWCHI", "KXLOWLA",
        "KXRAIN", "KXSNOW", "KXHURRICANE", "HURRICANE",
        "HIGHTEMP", "LOWTEMP", "TEMP",
    ]

    # Try fetching events with weather category
    for category in ["weather", "climate"]:
        time.sleep(config.REQUEST_DELAY)
        data = _get(
            f"{config.KALSHI_API_URL}/events",
            params={"status": "open", "series_ticker": category, "limit": 100},
        )
        if data and "events" in data:
            for event in data["events"]:
                for mkt in event.get("markets", []):
                    markets.append(mkt)

    # Try fetching markets directly with weather-related tickers
    for series in weather_series:
        time.sleep(config.REQUEST_DELAY)
        data = _get(
            f"{config.KALSHI_API_URL}/markets",
            params={"series_ticker": series, "limit": 100},
        )
        if data and "markets" in data:
            markets.extend(data["markets"])

    # Also try general market search
    for term in ["temperature", "weather", "hurricane", "snow"]:
        time.sleep(config.REQUEST_DELAY)
        data = _get(
            f"{config.KALSHI_API_URL}/markets",
            params={"status": "open", "limit": 50},
        )
        if data and "markets" in data:
            for mkt in data["markets"]:
                if _is_weather_market(mkt.get("title", ""), mkt.get("subtitle", "")):
                    markets.append(mkt)

    # Deduplicate
    seen = set()
    unique = []
    for m in markets:
        tid = m.get("ticker", "")
        if tid and tid not in seen:
            seen.add(tid)
            unique.append(m)

    print(f"  Found {len(unique)} weather-related markets on Kalshi")
    return unique


def parse_kalshi_markets(raw_markets):
    """Parse raw Kalshi market data into standardized rows."""
    rows = []
    now = datetime.now(timezone.utc).isoformat()

    for m in raw_markets:
        yes_price = m.get("yes_ask") or m.get("last_price") or m.get("yes_bid")
        if yes_price is not None:
            # Kalshi prices are in cents (0-100)
            price = float(yes_price) / 100.0 if float(yes_price) > 1 else float(yes_price)
        else:
            price = None

        resolved = m.get("status", "") in ("settled", "closed", "finalized")
        outcome = m.get("result") if resolved else None

        rows.append({
            "source": "kalshi",
            "market_id": m.get("ticker", ""),
            "question": m.get("title", ""),
            "description": m.get("subtitle", "")[:500] if m.get("subtitle") else "",
            "end_date": m.get("close_time") or m.get("expiration_time", ""),
            "resolved": resolved,
            "outcome": outcome,
            "volume": m.get("volume", 0),
            "last_price": price,
            "implied_prob": price,
            "fetched_at": now,
            "tags": m.get("category", ""),
        })
    return rows


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

MARKET_COLUMNS = [
    "source", "market_id", "question", "description", "end_date",
    "resolved", "outcome", "volume", "last_price", "implied_prob",
    "fetched_at", "tags",
]

PRICE_COLUMNS = ["source", "market_id", "timestamp", "price", "implied_prob", "volume"]


def load_existing_markets():
    """Load existing markets CSV into a DataFrame."""
    if os.path.exists(config.MARKETS_FILE):
        return pd.read_csv(config.MARKETS_FILE)
    return pd.DataFrame(columns=MARKET_COLUMNS)


def load_price_history():
    """Load existing price history."""
    if os.path.exists(config.PRICES_FILE):
        return pd.read_csv(config.PRICES_FILE)
    return pd.DataFrame(columns=PRICE_COLUMNS)


def save_markets(df):
    """Save markets DataFrame to CSV."""
    os.makedirs(os.path.dirname(config.MARKETS_FILE), exist_ok=True)
    df.to_csv(config.MARKETS_FILE, index=False)
    print(f"  Saved {len(df)} markets to {config.MARKETS_FILE}")


def save_price_history(df):
    """Save price history DataFrame to CSV."""
    os.makedirs(os.path.dirname(config.PRICES_FILE), exist_ok=True)
    df.to_csv(config.PRICES_FILE, index=False)
    print(f"  Saved {len(df)} price records to {config.PRICES_FILE}")


def update_markets(new_rows):
    """Merge new market data with existing, keeping latest snapshot per market."""
    existing = load_existing_markets()
    new_df = pd.DataFrame(new_rows, columns=MARKET_COLUMNS)

    if existing.empty:
        combined = new_df
    else:
        # For each (source, market_id), keep the latest fetched_at
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.sort_values("fetched_at", ascending=False)
        combined = combined.drop_duplicates(subset=["source", "market_id"], keep="first")

    save_markets(combined)
    return combined


def append_price_snapshot(new_rows):
    """Append new price snapshots to the rolling time series."""
    existing = load_price_history()
    now = datetime.now(timezone.utc).isoformat()

    price_rows = []
    for row in new_rows:
        if row.get("last_price") is not None:
            price_rows.append({
                "source": row["source"],
                "market_id": row["market_id"],
                "timestamp": now,
                "price": row["last_price"],
                "implied_prob": row["implied_prob"],
                "volume": row["volume"],
            })

    if price_rows:
        new_prices = pd.DataFrame(price_rows, columns=PRICE_COLUMNS)
        combined = pd.concat([existing, new_prices], ignore_index=True)
        save_price_history(combined)
        return combined

    return existing


def collect_all():
    """Run full collection pipeline. Returns list of standardized market rows."""
    all_rows = []

    # Polymarket
    try:
        poly_raw = fetch_polymarket_weather_markets()
        poly_rows = parse_polymarket_markets(poly_raw)
        all_rows.extend(poly_rows)
    except Exception as e:
        print(f"  [ERROR] Polymarket collection failed: {e}")

    # Kalshi
    try:
        kalshi_raw = fetch_kalshi_weather_markets()
        kalshi_rows = parse_kalshi_markets(kalshi_raw)
        all_rows.extend(kalshi_rows)
    except Exception as e:
        print(f"  [ERROR] Kalshi collection failed: {e}")

    print(f"\n[Total] Collected {len(all_rows)} weather market records")

    # Persist
    if all_rows:
        update_markets(all_rows)
        append_price_snapshot(all_rows)

    return all_rows
