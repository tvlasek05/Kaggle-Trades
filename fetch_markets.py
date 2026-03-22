"""Fetch weather prediction market data from Kalshi and Polymarket."""

import json
import time
import logging
from datetime import datetime, timezone

import requests

import config

logger = logging.getLogger(__name__)


def _request_with_retry(url, params=None, headers=None):
    """Make HTTP GET with retry logic."""
    for attempt in range(config.REQUEST_RETRIES):
        try:
            resp = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=config.REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.json()
            logger.warning("HTTP %d from %s", resp.status_code, url)
        except requests.RequestException as e:
            logger.warning("Request error (attempt %d): %s", attempt + 1, e)
        if attempt < config.REQUEST_RETRIES - 1:
            time.sleep(config.RETRY_BACKOFF * (attempt + 1))
    return None


# ---------------------------------------------------------------------------
# Kalshi
# ---------------------------------------------------------------------------

def fetch_kalshi_events(series_ticker):
    """Fetch events for a Kalshi weather series."""
    url = f"{config.KALSHI_BASE_URL}/events"
    data = _request_with_retry(url, params={"series_ticker": series_ticker, "limit": 100})
    if data and "events" in data:
        return data["events"]
    return []


def fetch_kalshi_markets(series_ticker):
    """Fetch all markets for a Kalshi weather series."""
    url = f"{config.KALSHI_BASE_URL}/markets"
    all_markets = []
    cursor = None

    for _ in range(20):  # page limit safety
        params = {"series_ticker": series_ticker, "limit": 200}
        if cursor:
            params["cursor"] = cursor
        data = _request_with_retry(url, params=params)
        if not data or "markets" not in data:
            break
        all_markets.extend(data["markets"])
        cursor = data.get("cursor")
        if not cursor:
            break

    return all_markets


def fetch_all_kalshi_weather():
    """Fetch all Kalshi weather markets and return normalized rows."""
    now = datetime.now(timezone.utc).isoformat()
    rows = []

    for series in config.KALSHI_WEATHER_SERIES:
        markets = fetch_kalshi_markets(series)
        logger.info("Kalshi %s: %d markets", series, len(markets))

        for m in markets:
            yes_price = m.get("yes_bid") or m.get("last_price") or 0
            no_price = m.get("no_bid") or (1 - yes_price) if yes_price else 0

            rows.append({
                "snapshot_time": now,
                "source": "kalshi",
                "market_id": m.get("ticker", ""),
                "event_id": m.get("event_ticker", ""),
                "series": series,
                "title": m.get("subtitle", m.get("title", "")),
                "yes_price": round(float(yes_price), 4),
                "no_price": round(float(no_price), 4),
                "yes_bid": float(m.get("yes_bid", 0) or 0),
                "yes_ask": float(m.get("yes_ask", 0) or 0),
                "volume": int(m.get("volume", 0) or 0),
                "open_interest": int(m.get("open_interest", 0) or 0),
                "status": m.get("status", ""),
                "result": m.get("result", ""),
                "expiration": m.get("expiration_time", ""),
                "floor_strike": m.get("floor_strike"),
                "cap_strike": m.get("cap_strike"),
            })

    return rows


# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------

def fetch_polymarket_weather_markets():
    """Fetch weather-related markets from Polymarket Gamma API."""
    rows = []
    now = datetime.now(timezone.utc).isoformat()
    seen_ids = set()

    # Try tag-based search
    for tag in config.POLYMARKET_WEATHER_TAGS:
        url = f"{config.POLYMARKET_GAMMA_URL}/markets"
        data = _request_with_retry(url, params={"tag": tag, "closed": "false", "limit": 100})
        if data and isinstance(data, list):
            for m in data:
                mid = m.get("id", "")
                if mid in seen_ids:
                    continue
                seen_ids.add(mid)
                rows.append(_normalize_polymarket(m, now))

    # Try keyword search
    for kw in config.POLYMARKET_WEATHER_KEYWORDS:
        url = f"{config.POLYMARKET_GAMMA_URL}/markets"
        data = _request_with_retry(url, params={"search": kw, "closed": "false", "limit": 50})
        if data and isinstance(data, list):
            for m in data:
                mid = m.get("id", "")
                if mid in seen_ids:
                    continue
                seen_ids.add(mid)
                rows.append(_normalize_polymarket(m, now))

    logger.info("Polymarket: %d weather markets found", len(rows))
    return rows


def _normalize_polymarket(m, now):
    """Normalize a Polymarket market object to a flat row."""
    outcome_prices = []
    try:
        outcome_prices = json.loads(m.get("outcomePrices", "[]"))
    except (json.JSONDecodeError, TypeError):
        pass

    yes_price = float(outcome_prices[0]) if len(outcome_prices) > 0 else 0
    no_price = float(outcome_prices[1]) if len(outcome_prices) > 1 else 0

    return {
        "snapshot_time": now,
        "source": "polymarket",
        "market_id": m.get("id", ""),
        "event_id": m.get("eventSlug", ""),
        "series": "",
        "title": m.get("question", ""),
        "yes_price": round(yes_price, 4),
        "no_price": round(no_price, 4),
        "yes_bid": 0.0,
        "yes_ask": 0.0,
        "volume": int(float(m.get("volume", 0) or 0)),
        "open_interest": 0,
        "status": "active" if m.get("active") else "closed",
        "result": "",
        "expiration": m.get("endDate", ""),
        "floor_strike": None,
        "cap_strike": None,
    }


def fetch_all_markets():
    """Fetch from all sources and return combined list of market snapshot rows."""
    kalshi_rows = fetch_all_kalshi_weather()
    poly_rows = fetch_polymarket_weather_markets()
    all_rows = kalshi_rows + poly_rows
    logger.info("Total market snapshots: %d", len(all_rows))
    return all_rows
