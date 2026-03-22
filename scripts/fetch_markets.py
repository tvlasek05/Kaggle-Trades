"""
Fetch weather prediction market data from Polymarket's public API.

Polymarket CLOB API: https://clob.polymarket.com
- GET /markets - list markets with filtering
- Weather markets are identified by tag/category search
"""

import requests
import json
import time
from datetime import datetime, timezone


POLYMARKET_BASE = "https://clob.polymarket.com"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"


def fetch_polymarket_weather_markets():
    """Fetch weather-related prediction markets from Polymarket's Gamma API."""
    markets = []
    weather_keywords = [
        "temperature", "weather", "hurricane", "tornado", "snowfall",
        "rainfall", "heat wave", "cold", "freeze", "precipitation",
        "storm", "flood", "drought", "wildfire", "celsius", "fahrenheit",
        "NOAA", "national weather", "climate"
    ]

    # Use Gamma API to search for weather-related markets
    for keyword in weather_keywords:
        try:
            resp = requests.get(
                f"{GAMMA_API_BASE}/markets",
                params={
                    "closed": "false",
                    "limit": 50,
                    "tag": keyword,
                },
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    markets.extend(data)
            time.sleep(0.3)  # rate limiting
        except requests.RequestException:
            pass

    # Also search by text query
    for keyword in ["weather", "temperature", "hurricane", "NOAA"]:
        try:
            resp = requests.get(
                f"{GAMMA_API_BASE}/markets",
                params={
                    "closed": "false",
                    "limit": 50,
                    "text_query": keyword,
                },
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    markets.extend(data)
            time.sleep(0.3)
        except requests.RequestException:
            pass

    # Deduplicate by condition_id
    seen = set()
    unique_markets = []
    for m in markets:
        cid = m.get("condition_id") or m.get("id", "")
        if cid and cid not in seen:
            seen.add(cid)
            unique_markets.append(m)

    return unique_markets


def fetch_kalshi_weather_markets():
    """Fetch weather-related event contracts from Kalshi's public API."""
    markets = []
    base = "https://api.elections.kalshi.com/trade-api/v2"

    weather_series = [
        "KXHIGHNY", "KXHIGHCHI", "KXHIGHLA", "KXHIGHMIA",
        "KXLOWNY", "KXLOWCHI", "KXLOWLA",
        "KXRAIN", "KXSNOW", "KXHURRICANE",
        "HIGHNY", "HIGHCHI", "HIGHLA", "HIGHMIA",
        "LOWNY", "LOWCHI", "LOWMIA",
        "RAIN", "SNOW", "HURRICANE",
    ]

    # Try fetching events with weather-related tickers
    for series in weather_series:
        try:
            resp = requests.get(
                f"{base}/markets",
                params={
                    "series_ticker": series,
                    "limit": 50,
                    "status": "open",
                },
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                if "markets" in data:
                    markets.extend(data["markets"])
            time.sleep(0.3)
        except requests.RequestException:
            pass

    # Also try the events endpoint with category filter
    try:
        resp = requests.get(
            f"{base}/events",
            params={"category": "climate", "limit": 100},
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            for event in data.get("events", []):
                for market in event.get("markets", []):
                    markets.append(market)
    except requests.RequestException:
        pass

    # Deduplicate
    seen = set()
    unique = []
    for m in markets:
        tid = m.get("ticker", "")
        if tid and tid not in seen:
            seen.add(tid)
            unique.append(m)

    return unique


def normalize_market(raw, source):
    """Normalize market data into a standard format."""
    if source == "polymarket":
        # Extract price from tokens if available
        price_yes = None
        tokens = raw.get("tokens", [])
        for t in tokens:
            if t.get("outcome", "").lower() == "yes":
                price_yes = t.get("price")
                break

        if price_yes is None:
            price_yes = raw.get("outcomePrices")
            if isinstance(price_yes, str):
                try:
                    prices = json.loads(price_yes)
                    price_yes = float(prices[0]) if prices else None
                except (json.JSONDecodeError, IndexError):
                    price_yes = None
            elif isinstance(price_yes, list) and len(price_yes) > 0:
                price_yes = float(price_yes[0])

        return {
            "source": "polymarket",
            "market_id": raw.get("condition_id", raw.get("id", "")),
            "question": raw.get("question", ""),
            "description": raw.get("description", ""),
            "end_date": raw.get("end_date_iso", raw.get("endDate", "")),
            "price_yes": float(price_yes) if price_yes is not None else None,
            "volume": raw.get("volume", 0),
            "liquidity": raw.get("liquidity", 0),
            "resolved": raw.get("closed", False),
            "resolution": raw.get("resolution", None),
            "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
            "raw_tags": str(raw.get("tags", [])),
        }

    elif source == "kalshi":
        yes_price = raw.get("yes_ask") or raw.get("last_price") or raw.get("yes_bid")
        return {
            "source": "kalshi",
            "market_id": raw.get("ticker", ""),
            "question": raw.get("title", raw.get("subtitle", "")),
            "description": raw.get("rules_primary", ""),
            "end_date": raw.get("expiration_time", raw.get("close_time", "")),
            "price_yes": float(yes_price) / 100 if yes_price else None,
            "volume": raw.get("volume", 0),
            "liquidity": raw.get("open_interest", 0),
            "resolved": raw.get("status", "") == "settled",
            "resolution": raw.get("result", None),
            "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
            "raw_tags": raw.get("category", ""),
        }

    return None


def fetch_all_weather_markets():
    """Fetch and normalize weather markets from all sources."""
    all_markets = []

    print("Fetching Polymarket weather markets...")
    poly_raw = fetch_polymarket_weather_markets()
    print(f"  Found {len(poly_raw)} Polymarket markets")
    for raw in poly_raw:
        normalized = normalize_market(raw, "polymarket")
        if normalized:
            all_markets.append(normalized)

    print("Fetching Kalshi weather markets...")
    kalshi_raw = fetch_kalshi_weather_markets()
    print(f"  Found {len(kalshi_raw)} Kalshi markets")
    for raw in kalshi_raw:
        normalized = normalize_market(raw, "kalshi")
        if normalized:
            all_markets.append(normalized)

    print(f"Total normalized markets: {len(all_markets)}")
    return all_markets


if __name__ == "__main__":
    markets = fetch_all_weather_markets()
    for m in markets[:5]:
        print(json.dumps(m, indent=2))
