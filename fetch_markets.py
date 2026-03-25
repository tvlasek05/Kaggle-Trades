"""Fetch weather prediction market data from Polymarket and Kalshi."""

import time
import requests
import pandas as pd
from datetime import datetime, timezone

import config


def _request_with_retry(url, params=None, headers=None):
    """Make HTTP GET with retries and exponential backoff."""
    for attempt in range(config.MAX_RETRIES):
        try:
            resp = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=config.REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == config.MAX_RETRIES - 1:
                print(f"  [WARN] Failed after {config.MAX_RETRIES} attempts: {url} — {e}")
                return None
            time.sleep(config.RETRY_BACKOFF * (2 ** attempt))
    return None


# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------

def fetch_polymarket_weather_markets():
    """Search Polymarket Gamma API for weather-related event markets."""
    markets = []
    for keyword in config.WEATHER_KEYWORDS:
        data = _request_with_retry(
            f"{config.POLYMARKET_GAMMA_API}/events",
            params={"closed": "false", "limit": 50, "title": keyword},
        )
        if not data:
            continue
        for event in data:
            for market in event.get("markets", []):
                markets.append({
                    "source": "polymarket",
                    "market_id": market.get("id", ""),
                    "condition_id": market.get("conditionId", ""),
                    "event_id": event.get("id", ""),
                    "title": market.get("question", event.get("title", "")),
                    "description": market.get("description", ""),
                    "outcome_yes_price": _safe_float(market.get("outcomePrices", "[]"), 0),
                    "outcome_no_price": _safe_float(market.get("outcomePrices", "[]"), 1),
                    "volume": _safe_float(market.get("volume", 0)),
                    "liquidity": _safe_float(market.get("liquidity", 0)),
                    "end_date": market.get("endDate", ""),
                    "resolved": market.get("resolved", False),
                    "resolution": market.get("resolution", ""),
                    "active": market.get("active", True),
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                })

    # Also search closed/resolved markets for calibration data
    for keyword in config.WEATHER_KEYWORDS[:5]:  # Limit to avoid rate issues
        data = _request_with_retry(
            f"{config.POLYMARKET_GAMMA_API}/events",
            params={"closed": "true", "limit": 50, "title": keyword},
        )
        if not data:
            continue
        for event in data:
            for market in event.get("markets", []):
                markets.append({
                    "source": "polymarket",
                    "market_id": market.get("id", ""),
                    "condition_id": market.get("conditionId", ""),
                    "event_id": event.get("id", ""),
                    "title": market.get("question", event.get("title", "")),
                    "description": market.get("description", ""),
                    "outcome_yes_price": _safe_float(market.get("outcomePrices", "[]"), 0),
                    "outcome_no_price": _safe_float(market.get("outcomePrices", "[]"), 1),
                    "volume": _safe_float(market.get("volume", 0)),
                    "liquidity": _safe_float(market.get("liquidity", 0)),
                    "end_date": market.get("endDate", ""),
                    "resolved": market.get("resolved", False),
                    "resolution": market.get("resolution", ""),
                    "active": market.get("active", False),
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                })

    # Deduplicate by market_id
    seen = set()
    unique = []
    for m in markets:
        if m["market_id"] not in seen:
            seen.add(m["market_id"])
            unique.append(m)
    return unique


def _safe_float(val, idx=None):
    """Parse a float from a value, optionally indexing into a JSON array string."""
    import json
    try:
        if idx is not None and isinstance(val, str):
            arr = json.loads(val)
            return float(arr[idx])
        return float(val)
    except (ValueError, TypeError, IndexError, json.JSONDecodeError):
        return 0.0


# ---------------------------------------------------------------------------
# Kalshi
# ---------------------------------------------------------------------------

def fetch_kalshi_weather_markets():
    """Fetch weather-related markets from Kalshi public API."""
    markets = []
    # Kalshi organizes by series tickers; common weather ones:
    weather_series = [
        "KXHIGHNY", "KXHIGHLA", "KXHIGHCHI",  # Temperature highs
        "KXLOWNY", "KXLOWCHI",                  # Temperature lows
        "KXHURR", "KXHURRCAT",                   # Hurricanes
        "KXSNOW", "KXSNOWNY",                    # Snowfall
        "KXRAIN",                                 # Rainfall
    ]

    # Try to search by keyword via the markets endpoint
    for keyword in ["weather", "temperature", "hurricane", "snow"]:
        data = _request_with_retry(
            f"{config.KALSHI_API}/markets",
            params={"status": "open", "limit": 100, "title": keyword},
        )
        if data and "markets" in data:
            for m in data["markets"]:
                markets.append(_parse_kalshi_market(m))

    # Also fetch by known series tickers
    for series in weather_series:
        data = _request_with_retry(
            f"{config.KALSHI_API}/markets",
            params={"series_ticker": series, "limit": 100},
        )
        if data and "markets" in data:
            for m in data["markets"]:
                markets.append(_parse_kalshi_market(m))

    # Deduplicate
    seen = set()
    unique = []
    for m in markets:
        if m["market_id"] not in seen:
            seen.add(m["market_id"])
            unique.append(m)
    return unique


def _parse_kalshi_market(m):
    """Parse a Kalshi market dict into our standard format."""
    yes_price = m.get("yes_ask", m.get("last_price", 0)) or 0
    no_price = m.get("no_ask", 0) or 0
    # Kalshi prices are in cents (0-100), normalize to 0-1
    if yes_price > 1:
        yes_price /= 100.0
    if no_price > 1:
        no_price /= 100.0

    return {
        "source": "kalshi",
        "market_id": m.get("ticker", ""),
        "condition_id": m.get("event_ticker", ""),
        "event_id": m.get("event_ticker", ""),
        "title": m.get("title", ""),
        "description": m.get("subtitle", ""),
        "outcome_yes_price": yes_price,
        "outcome_no_price": no_price,
        "volume": m.get("volume", 0) or 0,
        "liquidity": m.get("open_interest", 0) or 0,
        "end_date": m.get("expiration_time", m.get("close_time", "")),
        "resolved": m.get("status", "") == "settled",
        "resolution": m.get("result", ""),
        "active": m.get("status", "") in ("open", "active"),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Main fetch orchestrator
# ---------------------------------------------------------------------------

def fetch_all_weather_markets():
    """Fetch from all sources, return combined list."""
    print("Fetching Polymarket weather markets...")
    poly = fetch_polymarket_weather_markets()
    print(f"  Found {len(poly)} Polymarket markets")

    print("Fetching Kalshi weather markets...")
    kalshi = fetch_kalshi_weather_markets()
    print(f"  Found {len(kalshi)} Kalshi markets")

    all_markets = poly + kalshi
    print(f"Total: {len(all_markets)} weather markets")
    return all_markets


def save_markets(markets):
    """Append new market snapshots to the persistent markets CSV."""
    df_new = pd.DataFrame(markets)
    if df_new.empty:
        print("No markets to save.")
        return pd.DataFrame()

    try:
        df_existing = pd.read_csv(config.MARKETS_FILE)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    except FileNotFoundError:
        df_combined = df_new

    # Keep latest snapshot per market, but also save to price history
    df_combined.to_csv(config.MARKETS_FILE, index=False)
    print(f"Saved {len(df_new)} market snapshots to {config.MARKETS_FILE}")
    return df_combined


def save_price_history(markets):
    """Append price snapshots to the rolling time series."""
    records = []
    for m in markets:
        records.append({
            "timestamp": m["fetched_at"],
            "source": m["source"],
            "market_id": m["market_id"],
            "title": m["title"],
            "yes_price": m["outcome_yes_price"],
            "no_price": m["outcome_no_price"],
            "volume": m["volume"],
            "liquidity": m["liquidity"],
            "resolved": m["resolved"],
        })

    df_new = pd.DataFrame(records)
    if df_new.empty:
        return pd.DataFrame()

    try:
        df_existing = pd.read_csv(config.PRICES_FILE)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    except FileNotFoundError:
        df_combined = df_new

    df_combined.to_csv(config.PRICES_FILE, index=False)
    print(f"Appended {len(df_new)} price records to {config.PRICES_FILE}")
    return df_combined


if __name__ == "__main__":
    markets = fetch_all_weather_markets()
    save_markets(markets)
    save_price_history(markets)
