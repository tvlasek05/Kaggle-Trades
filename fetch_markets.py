"""Fetch weather prediction market data from Polymarket and Kalshi."""

import time
import requests
import pandas as pd
from datetime import datetime, timezone

import config


# Track which hosts are unreachable to skip retries in sandboxed environments
_unreachable_hosts = set()


def _request_with_retry(url, params=None, headers=None):
    """Make HTTP GET with retries and exponential backoff.

    Tracks unreachable hosts to avoid slow repeated timeouts in sandboxed envs.
    """
    from urllib.parse import urlparse
    host = urlparse(url).hostname
    if host in _unreachable_hosts:
        return None

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
            err_str = str(e).lower()
            # If proxy/connection blocked, mark host unreachable and bail immediately
            if "proxy" in err_str or "tunnel" in err_str or "forbidden" in err_str:
                print(f"  [WARN] Host unreachable (blocked): {host}")
                _unreachable_hosts.add(host)
                return None
            if attempt == config.MAX_RETRIES - 1:
                print(f"  [WARN] Failed after {config.MAX_RETRIES} attempts: {url} — {e}")
                return None
            time.sleep(config.RETRY_BACKOFF * (2 ** attempt))
    return None


# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------

def fetch_polymarket_weather_markets():
    """Search Polymarket Gamma API for weather-related markets.

    Uses both tag-based filtering (weather, climate) and keyword search.
    Gamma API requires no authentication; rate limit is ~400 req/sec.
    """
    markets = []

    def _collect_events(params):
        data = _request_with_retry(
            f"{config.POLYMARKET_GAMMA_API}/events", params=params,
        )
        if not data:
            return
        for event in data:
            for market in event.get("markets", []):
                markets.append(_parse_polymarket_market(market, event))

    def _collect_markets(params):
        data = _request_with_retry(
            f"{config.POLYMARKET_GAMMA_API}/markets", params=params,
        )
        if not data:
            return
        for market in (data if isinstance(data, list) else [data]):
            markets.append(_parse_polymarket_market(market))

    # Primary: tag-based queries (most reliable)
    for tag in ["weather", "climate", "temperature", "precipitation",
                "hurricane-season", "hurricanes"]:
        _collect_markets({"tag_slug": tag, "active": "true", "limit": 100})
        _collect_markets({"tag_slug": tag, "closed": "true", "limit": 50})

    # Secondary: keyword search via events endpoint
    for keyword in config.WEATHER_KEYWORDS:
        _collect_events({"closed": "false", "limit": 50, "title": keyword})

    # Also fetch resolved markets for calibration (top keywords only)
    for keyword in config.WEATHER_KEYWORDS[:5]:
        _collect_events({"closed": "true", "limit": 50, "title": keyword})

    # Deduplicate by market_id
    seen = set()
    unique = []
    for m in markets:
        mid = m["market_id"]
        if mid and mid not in seen:
            seen.add(mid)
            unique.append(m)
    return unique


def _parse_polymarket_market(market, event=None):
    """Parse a Polymarket Gamma API market response into our standard format.

    Gamma API returns outcomePrices as a JSON-stringified array like '[0.6, 0.4]'.
    """
    return {
        "source": "polymarket",
        "market_id": market.get("id", ""),
        "condition_id": market.get("conditionId", ""),
        "event_id": event.get("id", "") if event else "",
        "title": market.get("question", event.get("title", "") if event else ""),
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
    }


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
    """Fetch weather-related markets from Kalshi public API.

    No authentication needed for read-only market data.
    Uses series_ticker filtering for known weather series (KXHIGHNY, etc.)
    and also discovers new series via the /series endpoint.
    """
    markets = []

    # Fetch by known weather series tickers from config
    for series in config.KALSHI_WEATHER_SERIES:
        data = _request_with_retry(
            f"{config.KALSHI_API}/markets",
            params={"series_ticker": series, "limit": 200},
        )
        if data and "markets" in data:
            for m in data["markets"]:
                markets.append(_parse_kalshi_market(m))

    # Also try to discover additional weather series
    series_data = _request_with_retry(f"{config.KALSHI_API}/series")
    if series_data and "series" in series_data:
        for s in series_data["series"]:
            title = (s.get("title", "") + " " + s.get("category", "")).lower()
            if any(kw in title for kw in ["weather", "temperature", "climate",
                                           "hurricane", "snow", "rain"]):
                ticker = s.get("ticker", "")
                if ticker and ticker not in config.KALSHI_WEATHER_SERIES:
                    data = _request_with_retry(
                        f"{config.KALSHI_API}/markets",
                        params={"series_ticker": ticker, "limit": 100},
                    )
                    if data and "markets" in data:
                        for m in data["markets"]:
                            markets.append(_parse_kalshi_market(m))

    # Deduplicate
    seen = set()
    unique = []
    for m in markets:
        mid = m["market_id"]
        if mid and mid not in seen:
            seen.add(mid)
            unique.append(m)
    return unique


def _parse_kalshi_market(m):
    """Parse a Kalshi market dict into our standard format.

    Kalshi API uses dollar-denominated string fields (*_dollars, *_fp)
    which are already in 0-1 range (e.g., "0.5600" = 56 cents).
    """
    # Prefer new dollar-denominated fields, fall back to legacy cent fields
    yes_price = _safe_float(m.get("yes_ask_dollars")) or _safe_float(m.get("last_price_dollars")) or 0
    no_price = _safe_float(m.get("no_ask_dollars")) or 0

    # Legacy fallback: cent-based fields (0-100 range)
    if yes_price == 0:
        yes_price = (m.get("yes_ask") or m.get("last_price") or 0)
        if yes_price > 1:
            yes_price /= 100.0
    if no_price == 0:
        no_price = m.get("no_ask", 0) or 0
        if no_price > 1:
            no_price /= 100.0

    volume = _safe_float(m.get("volume_fp")) or m.get("volume", 0) or 0
    open_interest = _safe_float(m.get("open_interest_fp")) or m.get("open_interest", 0) or 0

    status = m.get("status", "")
    return {
        "source": "kalshi",
        "market_id": m.get("ticker", ""),
        "condition_id": m.get("event_ticker", ""),
        "event_id": m.get("event_ticker", ""),
        "title": m.get("title", ""),
        "description": m.get("yes_sub_title", m.get("subtitle", "")),
        "outcome_yes_price": yes_price,
        "outcome_no_price": no_price,
        "volume": volume,
        "liquidity": open_interest,
        "end_date": m.get("close_time", m.get("expiration_time", "")),
        "resolved": status in ("determined", "finalized"),
        "resolution": m.get("result", ""),
        "active": status in ("open", "active"),
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
