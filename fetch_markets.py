"""Fetch weather prediction market data from Polymarket and Kalshi."""

import time
import logging
import requests
import pandas as pd
from datetime import datetime, timezone

import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------

def _is_weather_market(text: str) -> bool:
    """Check if market text contains weather-related keywords."""
    lower = text.lower()
    return any(kw in lower for kw in config.WEATHER_KEYWORDS)


def fetch_polymarket_weather_markets() -> list[dict]:
    """Fetch weather-related markets from Polymarket Gamma API."""
    markets = []
    offset = 0
    limit = 100

    while True:
        try:
            resp = requests.get(
                f"{config.POLYMARKET_GAMMA_URL}/markets",
                params={"limit": limit, "offset": offset, "active": True},
                timeout=30,
            )
            resp.raise_for_status()
            batch = resp.json()
        except requests.RequestException as e:
            logger.warning("Polymarket fetch failed at offset %d: %s", offset, e)
            break

        if not batch:
            break

        for m in batch:
            question = m.get("question", "")
            description = m.get("description", "")
            if _is_weather_market(question) or _is_weather_market(description):
                markets.append({
                    "source": "polymarket",
                    "market_id": m.get("id", ""),
                    "condition_id": m.get("conditionId", ""),
                    "question": question,
                    "description": description[:500],
                    "outcome_yes_price": _safe_float(m.get("outcomePrices", "[]"), 0),
                    "outcome_no_price": _safe_float(m.get("outcomePrices", "[]"), 1),
                    "volume": _safe_float_val(m.get("volume", 0)),
                    "liquidity": _safe_float_val(m.get("liquidity", 0)),
                    "end_date": m.get("endDate", ""),
                    "resolved": m.get("closed", False),
                    "resolution": m.get("resolution", ""),
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                })

        offset += limit
        if len(batch) < limit:
            break
        time.sleep(0.5)  # rate limiting

    logger.info("Fetched %d weather markets from Polymarket", len(markets))
    return markets


def _safe_float(prices_str, idx):
    """Extract float from JSON-encoded price array string."""
    try:
        import json
        if isinstance(prices_str, str):
            prices = json.loads(prices_str)
        else:
            prices = prices_str
        if isinstance(prices, list) and len(prices) > idx:
            return float(prices[idx])
    except (ValueError, TypeError, json.JSONDecodeError):
        pass
    return None


def _safe_float_val(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Kalshi
# ---------------------------------------------------------------------------

def fetch_kalshi_weather_markets() -> list[dict]:
    """Fetch weather-related markets from Kalshi."""
    markets = []
    cursor = None

    # Kalshi weather series prefixes
    weather_series = [
        "KXHIGHNY", "KXHIGHLA", "KXHIGHCHI", "KXHIGHMIA",  # temperature highs
        "KXLOWNY", "KXLOWLA", "KXLOWCHI",  # temperature lows
        "KXSNOW", "KXRAIN",  # precipitation
        "KXHURR",  # hurricanes
        "HIGHTEMP", "LOWTEMP",  # temperature records
        "SNOW", "RAIN", "HURRICANE", "TORNADO",
    ]

    while True:
        params = {"limit": 200, "status": "open"}
        if cursor:
            params["cursor"] = cursor

        try:
            resp = requests.get(
                f"{config.KALSHI_BASE_URL}/markets",
                params=params,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.warning("Kalshi fetch failed: %s", e)
            break

        batch = data.get("markets", [])
        if not batch:
            break

        for m in batch:
            ticker = m.get("ticker", "")
            title = m.get("title", "")
            subtitle = m.get("subtitle", "")
            series_ticker = m.get("series_ticker", "")

            is_weather = (
                any(s in series_ticker.upper() for s in weather_series)
                or any(s in ticker.upper() for s in weather_series)
                or _is_weather_market(title)
                or _is_weather_market(subtitle)
            )

            if is_weather:
                yes_price = _safe_float_val(m.get("yes_ask", m.get("last_price", 0)))
                no_price = _safe_float_val(m.get("no_ask", 0))
                if yes_price and yes_price > 1:
                    # Kalshi prices in cents
                    yes_price = yes_price / 100.0
                    no_price = no_price / 100.0 if no_price else None

                markets.append({
                    "source": "kalshi",
                    "market_id": ticker,
                    "condition_id": m.get("event_ticker", ""),
                    "question": title,
                    "description": subtitle[:500] if subtitle else "",
                    "outcome_yes_price": yes_price,
                    "outcome_no_price": no_price,
                    "volume": _safe_float_val(m.get("volume", 0)),
                    "liquidity": _safe_float_val(m.get("open_interest", 0)),
                    "end_date": m.get("close_time", m.get("expiration_time", "")),
                    "resolved": m.get("status", "") == "settled",
                    "resolution": m.get("result", ""),
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                })

        cursor = data.get("cursor")
        if not cursor or len(batch) < 200:
            break
        time.sleep(0.5)

    logger.info("Fetched %d weather markets from Kalshi", len(markets))
    return markets


# ---------------------------------------------------------------------------
# Combined
# ---------------------------------------------------------------------------

def fetch_all_weather_markets() -> pd.DataFrame:
    """Fetch weather markets from all sources and return as DataFrame."""
    all_markets = []

    poly_markets = fetch_polymarket_weather_markets()
    all_markets.extend(poly_markets)

    kalshi_markets = fetch_kalshi_weather_markets()
    all_markets.extend(kalshi_markets)

    if not all_markets:
        logger.warning("No weather markets found from any source")
        return pd.DataFrame()

    df = pd.DataFrame(all_markets)
    logger.info("Total weather markets fetched: %d", len(df))
    return df
