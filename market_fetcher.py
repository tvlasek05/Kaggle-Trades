"""
Fetch weather prediction market data from Polymarket and Kalshi.
"""

import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
MARKET_PRICES_FILE = os.path.join(DATA_DIR, "market_prices.csv")
MARKETS_META_FILE = os.path.join(DATA_DIR, "markets_metadata.json")

WEATHER_KEYWORDS = [
    "temperature", "weather", "hurricane", "snow", "rain", "heat",
    "cold", "storm", "tornado", "flood", "drought", "wildfire",
    "celsius", "fahrenheit", "climate", "el nino", "la nina",
    "precipitation", "wind", "heatwave", "heat wave", "freeze",
    "blizzard", "cyclone", "typhoon", "monsoon", "ice",
]

# --- Polymarket ---

POLYMARKET_BASE = "https://clob.polymarket.com"
POLYMARKET_GAMMA_BASE = "https://gamma-api.polymarket.com"


def fetch_polymarket_weather_markets():
    """Fetch weather-related markets from Polymarket's Gamma API."""
    markets = []
    for keyword in WEATHER_KEYWORDS[:10]:  # Top keywords to avoid rate limits
        try:
            resp = requests.get(
                f"{POLYMARKET_GAMMA_BASE}/markets",
                params={"closed": "false", "limit": 50, "search": keyword},
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                for m in data:
                    markets.append(_parse_polymarket(m))
            time.sleep(0.5)  # Rate limit respect
        except Exception as e:
            print(f"  [Polymarket] Error searching '{keyword}': {e}")

    # Also fetch closed/resolved markets for calibration
    for keyword in WEATHER_KEYWORDS[:5]:
        try:
            resp = requests.get(
                f"{POLYMARKET_GAMMA_BASE}/markets",
                params={"closed": "true", "limit": 50, "search": keyword},
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                for m in data:
                    parsed = _parse_polymarket(m)
                    parsed["resolved"] = True
                    markets.append(parsed)
            time.sleep(0.5)
        except Exception as e:
            print(f"  [Polymarket] Error searching resolved '{keyword}': {e}")

    # Deduplicate by condition_id
    seen = set()
    unique = []
    for m in markets:
        if m["market_id"] not in seen:
            seen.add(m["market_id"])
            unique.append(m)
    return unique


def _parse_polymarket(m):
    """Parse a Polymarket market object into our standard format."""
    outcome_prices = {}
    outcomes = m.get("outcomes", "")
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except (json.JSONDecodeError, TypeError):
            outcomes = []

    prices = m.get("outcomePrices", "")
    if isinstance(prices, str):
        try:
            prices = json.loads(prices)
        except (json.JSONDecodeError, TypeError):
            prices = []

    for i, outcome in enumerate(outcomes):
        if i < len(prices):
            try:
                outcome_prices[outcome] = float(prices[i])
            except (ValueError, TypeError):
                pass

    return {
        "source": "polymarket",
        "market_id": m.get("conditionId", m.get("id", "")),
        "slug": m.get("slug", ""),
        "question": m.get("question", ""),
        "description": m.get("description", "")[:500],
        "end_date": m.get("endDate", ""),
        "resolved": m.get("closed", False),
        "resolution": m.get("resolvedBy", ""),
        "outcome_prices": outcome_prices,
        "volume": m.get("volume", 0),
        "liquidity": m.get("liquidity", 0),
        "category": "weather",
    }


# --- Kalshi ---

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"


def fetch_kalshi_weather_markets():
    """Fetch weather-related event markets from Kalshi public API."""
    markets = []
    weather_series = [
        "KXHIGHNY", "KXHIGHLA", "KXHIGHCHI", "KXHIGHMIA",  # Temperature highs
        "KXLOWNY", "KXLOWCHI",  # Temperature lows
        "KXSNOWNY", "KXSNOWCHI", "KXSNOWBOS",  # Snowfall
        "KXRAINNY", "KXRAINLA",  # Rainfall
        "KXHURRICANE", "HURRICANE",  # Hurricanes
        "HIGHTEMP", "TEMP",  # General temperature
    ]

    # Try fetching from events endpoint
    try:
        resp = requests.get(
            f"{KALSHI_BASE}/events",
            params={"limit": 100, "status": "open",
                    "series_ticker": ""},
            headers={"Accept": "application/json"},
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            events = data.get("events", [])
            for event in events:
                title = (event.get("title", "") + " " + event.get("category", "")).lower()
                if any(kw in title for kw in WEATHER_KEYWORDS):
                    for mkt in event.get("markets", []):
                        markets.append(_parse_kalshi(mkt, event))
    except Exception as e:
        print(f"  [Kalshi] Error fetching events: {e}")

    # Also try direct markets endpoint with weather series tickers
    for series in weather_series:
        try:
            resp = requests.get(
                f"{KALSHI_BASE}/markets",
                params={"limit": 50, "series_ticker": series, "status": "open"},
                headers={"Accept": "application/json"},
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                for m in data.get("markets", []):
                    markets.append(_parse_kalshi(m))
            time.sleep(0.3)
        except Exception as e:
            print(f"  [Kalshi] Error fetching series {series}: {e}")

    # Fetch settled markets for calibration
    for series in weather_series[:5]:
        try:
            resp = requests.get(
                f"{KALSHI_BASE}/markets",
                params={"limit": 50, "series_ticker": series, "status": "settled"},
                headers={"Accept": "application/json"},
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                for m in data.get("markets", []):
                    parsed = _parse_kalshi(m)
                    parsed["resolved"] = True
                    markets.append(parsed)
            time.sleep(0.3)
        except Exception as e:
            pass

    # Deduplicate
    seen = set()
    unique = []
    for m in markets:
        if m["market_id"] not in seen:
            seen.add(m["market_id"])
            unique.append(m)
    return unique


def _parse_kalshi(m, event=None):
    """Parse a Kalshi market into our standard format."""
    yes_price = m.get("yes_bid", m.get("last_price", 0))
    no_price = m.get("no_bid", 0)
    if yes_price is None:
        yes_price = 0
    if no_price is None:
        no_price = 0

    # Convert cents to dollars if needed
    if isinstance(yes_price, (int, float)) and yes_price > 1:
        yes_price = yes_price / 100.0
    if isinstance(no_price, (int, float)) and no_price > 1:
        no_price = no_price / 100.0

    result = m.get("result", "")

    return {
        "source": "kalshi",
        "market_id": m.get("ticker", m.get("id", "")),
        "slug": m.get("ticker", ""),
        "question": m.get("title", m.get("subtitle", "")),
        "description": (event.get("title", "") if event else m.get("title", ""))[:500],
        "end_date": m.get("expiration_time", m.get("close_time", "")),
        "resolved": m.get("status", "") == "settled",
        "resolution": result if result else "",
        "outcome_prices": {"Yes": yes_price, "No": no_price},
        "volume": m.get("volume", 0),
        "liquidity": m.get("open_interest", 0),
        "category": "weather",
    }


# --- Data persistence ---

def load_existing_prices():
    """Load existing price history from CSV."""
    if os.path.exists(MARKET_PRICES_FILE):
        return pd.read_csv(MARKET_PRICES_FILE)
    return pd.DataFrame(columns=[
        "timestamp", "source", "market_id", "question", "outcome",
        "price", "volume", "resolved", "resolution",
    ])


def load_existing_metadata():
    """Load market metadata JSON."""
    if os.path.exists(MARKETS_META_FILE):
        with open(MARKETS_META_FILE, "r") as f:
            return json.load(f)
    return {}


def save_prices(df):
    """Save price DataFrame to CSV."""
    os.makedirs(DATA_DIR, exist_ok=True)
    df.to_csv(MARKET_PRICES_FILE, index=False)


def save_metadata(meta):
    """Save metadata dict to JSON."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(MARKETS_META_FILE, "w") as f:
        json.dump(meta, f, indent=2, default=str)


def append_market_snapshots(markets):
    """Append current market prices to the rolling time series."""
    now = datetime.now(timezone.utc).isoformat()
    existing_df = load_existing_prices()
    metadata = load_existing_metadata()

    new_rows = []
    for m in markets:
        # Update metadata
        metadata[m["market_id"]] = {
            "source": m["source"],
            "question": m["question"],
            "description": m["description"],
            "slug": m["slug"],
            "end_date": m["end_date"],
            "category": m["category"],
        }

        # Add price rows for each outcome
        for outcome, price in m.get("outcome_prices", {}).items():
            new_rows.append({
                "timestamp": now,
                "source": m["source"],
                "market_id": m["market_id"],
                "question": m["question"][:200],
                "outcome": outcome,
                "price": price,
                "volume": m.get("volume", 0),
                "resolved": m.get("resolved", False),
                "resolution": m.get("resolution", ""),
            })

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        save_prices(combined)
        save_metadata(metadata)
        return len(new_rows)
    return 0


def fetch_all_weather_markets():
    """Fetch from all sources and return combined list."""
    all_markets = []

    print("Fetching Polymarket weather markets...")
    poly = fetch_polymarket_weather_markets()
    print(f"  Found {len(poly)} Polymarket markets")
    all_markets.extend(poly)

    print("Fetching Kalshi weather markets...")
    kalshi = fetch_kalshi_weather_markets()
    print(f"  Found {len(kalshi)} Kalshi markets")
    all_markets.extend(kalshi)

    return all_markets


if __name__ == "__main__":
    markets = fetch_all_weather_markets()
    n = append_market_snapshots(markets)
    print(f"\nAppended {n} price snapshots across {len(markets)} markets")
