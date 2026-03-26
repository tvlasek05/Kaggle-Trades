"""Fetch weather prediction market data from Polymarket and Manifold Markets."""

import csv
import time
from datetime import datetime, timezone

import requests

import config

POLYMARKET_GAMMA_URL = "https://gamma-api.polymarket.com/markets"
MANIFOLD_SEARCH_URL = "https://api.manifold.markets/v0/search-markets"

WEATHER_SEARCH_TERMS = [
    "weather", "temperature", "hurricane", "snowfall",
    "heat wave", "rainfall", "tornado", "tropical storm",
]

POLYMARKET_CSV = config.DATA_DIR / "polymarket.csv"
MANIFOLD_CSV = config.DATA_DIR / "manifold.csv"

FIELDNAMES_POLY = [
    "id", "question", "slug", "outcome_yes_price", "outcome_no_price",
    "volume", "active", "closed", "end_date", "source",
    "first_seen", "last_updated",
]

FIELDNAMES_MANIFOLD = [
    "id", "question", "slug", "probability", "volume",
    "is_resolved", "resolution", "close_time", "source",
    "first_seen", "last_updated",
]


def _safe_request(url: str, params: dict, timeout: int = 15) -> dict | list | None:
    """Make a GET request, return parsed JSON or None on failure."""
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        if resp.status_code == 429:
            time.sleep(5)
            resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"  Error fetching {url}: {e}")
        return None


def fetch_polymarket_weather() -> list[dict]:
    """Fetch weather-related markets from Polymarket Gamma API."""
    all_markets = []
    seen_ids = set()

    for term in WEATHER_SEARCH_TERMS:
        print(f"  Polymarket: searching '{term}'")
        data = _safe_request(POLYMARKET_GAMMA_URL, {
            "tag": term, "closed": "false", "limit": 100,
        })
        if not data:
            # Try text search as fallback
            data = _safe_request(POLYMARKET_GAMMA_URL, {
                "closed": "false", "limit": 50,
            })
        if not data:
            continue

        markets = data if isinstance(data, list) else data.get("markets", data.get("data", []))
        if not isinstance(markets, list):
            continue

        for mkt in markets:
            mid = str(mkt.get("id", mkt.get("conditionId", "")))
            if mid in seen_ids or not mid:
                continue
            seen_ids.add(mid)

            # Parse outcome prices
            prices = mkt.get("outcomePrices", "")
            yes_price, no_price = "", ""
            if isinstance(prices, list) and len(prices) >= 2:
                yes_price = prices[0]
                no_price = prices[1]
            elif isinstance(prices, str) and "," in prices:
                parts = prices.split(",")
                yes_price = parts[0].strip().strip('"[]')
                no_price = parts[1].strip().strip('"[]') if len(parts) > 1 else ""

            all_markets.append({
                "id": mid,
                "question": mkt.get("question", mkt.get("title", "")),
                "slug": mkt.get("slug", ""),
                "outcome_yes_price": yes_price,
                "outcome_no_price": no_price,
                "volume": mkt.get("volume", mkt.get("volumeNum", "")),
                "active": mkt.get("active", ""),
                "closed": mkt.get("closed", ""),
                "end_date": mkt.get("endDate", mkt.get("end_date_iso", "")),
                "source": "polymarket",
            })

        time.sleep(0.3)

    print(f"  Polymarket: {len(all_markets)} weather markets found")
    return all_markets


def fetch_manifold_weather() -> list[dict]:
    """Fetch weather-related markets from Manifold Markets."""
    all_markets = []
    seen_ids = set()

    for term in WEATHER_SEARCH_TERMS:
        print(f"  Manifold: searching '{term}'")
        data = _safe_request(MANIFOLD_SEARCH_URL, {
            "term": term, "sort": "newest", "limit": 50,
        })
        if not data or not isinstance(data, list):
            continue

        for mkt in data:
            mid = str(mkt.get("id", ""))
            if mid in seen_ids or not mid:
                continue
            seen_ids.add(mid)

            all_markets.append({
                "id": mid,
                "question": mkt.get("question", ""),
                "slug": mkt.get("slug", ""),
                "probability": mkt.get("probability", ""),
                "volume": mkt.get("volume", ""),
                "is_resolved": mkt.get("isResolved", ""),
                "resolution": mkt.get("resolution", ""),
                "close_time": mkt.get("closeTime", ""),
                "source": "manifold",
            })

        time.sleep(0.3)

    print(f"  Manifold: {len(all_markets)} weather markets found")
    return all_markets


def _save_csv(path, records, fieldnames):
    """Save records to CSV, merging with existing data."""
    now = datetime.now(timezone.utc).isoformat()
    existing = {}
    if path.exists():
        with open(path, "r") as f:
            for row in csv.DictReader(f):
                existing[row["id"]] = row

    for r in records:
        rid = r["id"]
        if rid in existing:
            r["first_seen"] = existing[rid].get("first_seen", now)
        else:
            r["first_seen"] = now
        r["last_updated"] = now
        existing[rid] = r

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in sorted(existing.values(), key=lambda r: r.get("id", "")):
            writer.writerow(row)

    return existing


def run():
    """Fetch weather markets from Polymarket and Manifold."""
    print("Fetching Polymarket weather markets...")
    poly = fetch_polymarket_weather()
    poly_saved = _save_csv(POLYMARKET_CSV, poly, FIELDNAMES_POLY)

    print("Fetching Manifold weather markets...")
    mani = fetch_manifold_weather()
    mani_saved = _save_csv(MANIFOLD_CSV, mani, FIELDNAMES_MANIFOLD)

    print(f"\nSupplementary markets: {len(poly_saved)} Polymarket, {len(mani_saved)} Manifold")
    return {"polymarket": poly_saved, "manifold": mani_saved}


if __name__ == "__main__":
    run()
