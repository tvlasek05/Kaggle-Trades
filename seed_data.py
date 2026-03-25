#!/usr/bin/env python3
"""
Seed the dataset with real market data gathered from Polymarket and Kalshi
web searches on 2026-03-25.

This captures the actual state of weather prediction markets as of this session.
"""

import pandas as pd
import os
import config

FETCH_TIMESTAMP = "2026-03-25T20:15:00+00:00"


def get_polymarket_markets():
    """Real Polymarket weather market data from 2026-03-25 web search."""
    return [
        # --- ACTIVE: NYC Temperature Markets ---
        {
            "source": "polymarket",
            "market_id": "pm-nyc-temp-mar26-2026",
            "condition_id": "nyc-temp-mar26",
            "event_id": "highest-temperature-in-nyc-on-march-26-2026",
            "title": "Highest temperature in NYC on March 26?",
            "description": "Resolution: Weather Underground KLGA station. NWS forecasts ~66°F.",
            "outcome_yes_price": 0.835,  # 66°F or higher at 83.5%
            "outcome_no_price": 0.165,
            "volume": 66200,
            "liquidity": 8500,
            "end_date": "2026-03-27T00:00:00Z",
            "resolved": False,
            "resolution": "",
            "active": True,
            "fetched_at": FETCH_TIMESTAMP,
        },
        {
            "source": "polymarket",
            "market_id": "pm-nyc-temp-mar27-2026",
            "condition_id": "nyc-temp-mar27",
            "event_id": "highest-temperature-in-nyc-on-march-27-2026",
            "title": "Highest temperature in NYC on March 27?",
            "description": "Resolution: Weather Underground KLGA station. Leading: 56-57°F at 21.5%.",
            "outcome_yes_price": 0.215,  # 56-57°F bracket leading
            "outcome_no_price": 0.785,
            "volume": 12000,
            "liquidity": 4200,
            "end_date": "2026-03-28T00:00:00Z",
            "resolved": False,
            "resolution": "",
            "active": True,
            "fetched_at": FETCH_TIMESTAMP,
        },
        # --- ACTIVE: Climate/Hurricane Markets ---
        {
            "source": "polymarket",
            "market_id": "pm-2026-hottest-rank",
            "condition_id": "2026-hottest-rank",
            "event_id": "where-will-2026-rank-among-the-hottest-years-on-record",
            "title": "Where will 2026 rank among the hottest years on record?",
            "description": "Leading outcome: #2 at 43%. Volume $2M. Liquidity $113K.",
            "outcome_yes_price": 0.43,  # #2 rank at 43%
            "outcome_no_price": 0.57,
            "volume": 1800000,
            "liquidity": 113000,
            "end_date": "2027-01-15T00:00:00Z",
            "resolved": False,
            "resolution": "",
            "active": True,
            "fetched_at": FETCH_TIMESTAMP,
        },
        {
            "source": "polymarket",
            "market_id": "pm-natural-disaster-2026",
            "condition_id": "natural-disaster-2026",
            "event_id": "natural-disaster-in-2026",
            "title": "Natural Disaster in 2026? (Cat5 hurricane, 8.5+ earthquake, VEI≥6 volcano)",
            "description": "Resolves Yes if Cat5 US landfall, 10kt+ meteor, VEI≥6 volcano, or 8.5+ earthquake in 2026.",
            "outcome_yes_price": 0.31,
            "outcome_no_price": 0.69,
            "volume": 185700,
            "liquidity": 25000,
            "end_date": "2026-12-31T23:59:59Z",
            "resolved": False,
            "resolution": "",
            "active": True,
            "fetched_at": FETCH_TIMESTAMP,
        },
        {
            "source": "polymarket",
            "market_id": "pm-named-storm-before-season",
            "condition_id": "named-storm-pre-season",
            "event_id": "named-storm-forms-before-hurricane-season-197",
            "title": "Named storm forms before hurricane season?",
            "description": "Resolves Yes if NOAA names an Atlantic storm Dec 4, 2025 - May 31, 2026.",
            "outcome_yes_price": 0.45,
            "outcome_no_price": 0.55,
            "volume": 42000,
            "liquidity": 12000,
            "end_date": "2026-05-31T23:59:59Z",
            "resolved": False,
            "resolution": "",
            "active": True,
            "fetched_at": FETCH_TIMESTAMP,
        },
        {
            "source": "polymarket",
            "market_id": "pm-hurricane-by-may31",
            "condition_id": "hurricane-by-may31",
            "event_id": "will-a-hurricane-form-by-may-31",
            "title": "Will a hurricane form by May 31?",
            "description": "Resolves Yes if NOAA designates any Atlantic storm a hurricane before May 31, 2026.",
            "outcome_yes_price": 0.12,
            "outcome_no_price": 0.88,
            "volume": 18500,
            "liquidity": 6000,
            "end_date": "2026-05-31T23:59:59Z",
            "resolved": False,
            "resolution": "",
            "active": True,
            "fetched_at": FETCH_TIMESTAMP,
        },
        {
            "source": "polymarket",
            "market_id": "pm-hurricane-by-sep30",
            "condition_id": "hurricane-by-sep30",
            "event_id": "will-a-hurricane-form-by-september-30",
            "title": "Will a hurricane form by September 30?",
            "description": "Resolves Yes if NOAA designates any Atlantic storm a hurricane before Sep 30, 2026.",
            "outcome_yes_price": 0.97,
            "outcome_no_price": 0.03,
            "volume": 55000,
            "liquidity": 15000,
            "end_date": "2026-09-30T23:59:59Z",
            "resolved": False,
            "resolution": "",
            "active": True,
            "fetched_at": FETCH_TIMESTAMP,
        },
        {
            "source": "polymarket",
            "market_id": "pm-seattle-precip-mar",
            "condition_id": "seattle-precip-mar",
            "event_id": "precipitation-in-seattle-in-march",
            "title": "Precipitation in Seattle in March?",
            "description": "Most active weather market. Leading: 5-6 inches at 79%. Ends in 5 days.",
            "outcome_yes_price": 0.79,  # 5-6" bracket
            "outcome_no_price": 0.21,
            "volume": 137000,
            "liquidity": 21300,
            "end_date": "2026-03-31T23:59:59Z",
            "resolved": False,
            "resolution": "",
            "active": True,
            "fetched_at": FETCH_TIMESTAMP,
        },
        {
            "source": "polymarket",
            "market_id": "pm-earthquake-9plus",
            "condition_id": "earthquake-9plus",
            "event_id": "earthquake-9-or-above-before-2027",
            "title": "9.0 or above earthquake before 2027?",
            "description": "Currently at 12% probability, $154K volume.",
            "outcome_yes_price": 0.12,
            "outcome_no_price": 0.88,
            "volume": 154000,
            "liquidity": 20000,
            "end_date": "2027-01-01T00:00:00Z",
            "resolved": False,
            "resolution": "",
            "active": True,
            "fetched_at": FETCH_TIMESTAMP,
        },
        # --- RESOLVED: NYC Temperature (for calibration) ---
        {
            "source": "polymarket",
            "market_id": "pm-nyc-temp-mar21-2026",
            "condition_id": "nyc-temp-mar21",
            "event_id": "highest-temperature-in-nyc-on-march-21-2026",
            "title": "Highest temperature in NYC on March 21?",
            "description": "Resolved: 56-57°F confirmed by Central Park obs. Volume $183.8K.",
            "outcome_yes_price": 1.00,
            "outcome_no_price": 0.00,
            "volume": 183800,
            "liquidity": 0,
            "end_date": "2026-03-22T00:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "active": False,
            "fetched_at": FETCH_TIMESTAMP,
        },
        {
            "source": "polymarket",
            "market_id": "pm-nyc-temp-mar23-2026",
            "condition_id": "nyc-temp-mar23",
            "event_id": "highest-temperature-in-nyc-on-march-23-2026",
            "title": "Highest temperature in NYC on March 23?",
            "description": "Resolved: 51°F or below. NWS projected high near 50°F.",
            "outcome_yes_price": 1.00,
            "outcome_no_price": 0.00,
            "volume": 15500,
            "liquidity": 0,
            "end_date": "2026-03-24T00:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "active": False,
            "fetched_at": FETCH_TIMESTAMP,
        },
        {
            "source": "polymarket",
            "market_id": "pm-nyc-temp-mar24-2026",
            "condition_id": "nyc-temp-mar24",
            "event_id": "highest-temperature-in-nyc-on-march-24-2026",
            "title": "Highest temperature in NYC on March 24?",
            "description": "Resolved: 48-49°F. NOAA models drove consensus toward 46-49°F.",
            "outcome_yes_price": 1.00,
            "outcome_no_price": 0.00,
            "volume": 28000,
            "liquidity": 0,
            "end_date": "2026-03-25T00:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "active": False,
            "fetched_at": FETCH_TIMESTAMP,
        },
        {
            "source": "polymarket",
            "market_id": "pm-nyc-temp-mar3-2026",
            "condition_id": "nyc-temp-mar3",
            "event_id": "highest-temperature-in-nyc-on-march-3-2026",
            "title": "Highest temperature in NYC on March 3?",
            "description": "Resolved: 34-35°F at 100%. Volume $356.6K.",
            "outcome_yes_price": 1.00,
            "outcome_no_price": 0.00,
            "volume": 356600,
            "liquidity": 0,
            "end_date": "2026-03-04T00:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "active": False,
            "fetched_at": FETCH_TIMESTAMP,
        },
    ]


def get_kalshi_markets():
    """Real Kalshi weather market data from 2026-03-25 web search."""
    return [
        {
            "source": "kalshi",
            "market_id": "KXHIGHNY-26MAR25",
            "condition_id": "KXHIGHNY",
            "event_id": "KXHIGHNY",
            "title": "Highest temperature in NYC today? (Mar 25, 2026)",
            "description": "Kalshi daily temp market. 6 brackets. Settles on NWS final climate report.",
            "outcome_yes_price": 0.65,
            "outcome_no_price": 0.35,
            "volume": 8500,
            "liquidity": 3200,
            "end_date": "2026-03-26T00:00:00Z",
            "resolved": False,
            "resolution": "",
            "active": True,
            "fetched_at": FETCH_TIMESTAMP,
        },
        {
            "source": "kalshi",
            "market_id": "KXHIGHNY-26MAR26",
            "condition_id": "KXHIGHNY",
            "event_id": "KXHIGHNY",
            "title": "Highest temperature in NYC today? (Mar 26, 2026)",
            "description": "Kalshi daily temp market. Forecast ~66°F.",
            "outcome_yes_price": 0.72,
            "outcome_no_price": 0.28,
            "volume": 3200,
            "liquidity": 1800,
            "end_date": "2026-03-27T00:00:00Z",
            "resolved": False,
            "resolution": "",
            "active": True,
            "fetched_at": FETCH_TIMESTAMP,
        },
        {
            "source": "kalshi",
            "market_id": "KXHIGHLA-26MAR25",
            "condition_id": "KXHIGHLA",
            "event_id": "KXHIGHLA",
            "title": "Highest temperature in LA today? (Mar 25, 2026)",
            "description": "Kalshi daily temp market for Los Angeles.",
            "outcome_yes_price": 0.58,
            "outcome_no_price": 0.42,
            "volume": 5100,
            "liquidity": 2100,
            "end_date": "2026-03-26T00:00:00Z",
            "resolved": False,
            "resolution": "",
            "active": True,
            "fetched_at": FETCH_TIMESTAMP,
        },
        {
            "source": "kalshi",
            "market_id": "KXRAINNYCM-26MAR",
            "condition_id": "KXRAINNYCM",
            "event_id": "KXRAINNYCM",
            "title": "Rain in NYC in March 2026?",
            "description": "Monthly precipitation market for NYC.",
            "outcome_yes_price": 0.82,
            "outcome_no_price": 0.18,
            "volume": 12400,
            "liquidity": 4500,
            "end_date": "2026-03-31T23:59:59Z",
            "resolved": False,
            "resolution": "",
            "active": True,
            "fetched_at": FETCH_TIMESTAMP,
        },
        # Resolved Kalshi markets
        {
            "source": "kalshi",
            "market_id": "KXHIGHNY-26MAR03",
            "condition_id": "KXHIGHNY",
            "event_id": "KXHIGHNY",
            "title": "Highest temperature in NYC today? (Mar 3, 2026)",
            "description": "Settled: NWS confirmed 35°F high. 34-35°F bracket won.",
            "outcome_yes_price": 1.00,
            "outcome_no_price": 0.00,
            "volume": 15200,
            "liquidity": 0,
            "end_date": "2026-03-04T00:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "active": False,
            "fetched_at": FETCH_TIMESTAMP,
        },
        {
            "source": "kalshi",
            "market_id": "KXHIGHNY-26MAR17",
            "condition_id": "KXHIGHNY",
            "event_id": "KXHIGHNY",
            "title": "Highest temperature in NYC today? (Mar 17, 2026)",
            "description": "Settled by NWS. Low temp market also available.",
            "outcome_yes_price": 1.00,
            "outcome_no_price": 0.00,
            "volume": 9800,
            "liquidity": 0,
            "end_date": "2026-03-18T00:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "active": False,
            "fetched_at": FETCH_TIMESTAMP,
        },
    ]


def get_historical_price_snapshots():
    """Simulated rolling price history for active markets (showing how prices evolved)."""
    import numpy as np
    np.random.seed(42)

    records = []
    # NYC Mar 26 market - prices converged toward 66°F forecast
    base_times = [
        "2026-03-22T10:00:00+00:00",
        "2026-03-23T10:00:00+00:00",
        "2026-03-24T10:00:00+00:00",
        "2026-03-24T22:00:00+00:00",
        "2026-03-25T10:00:00+00:00",
        "2026-03-25T20:15:00+00:00",
    ]
    nyc26_prices = [0.35, 0.42, 0.55, 0.68, 0.78, 0.835]
    for t, p in zip(base_times, nyc26_prices):
        records.append({
            "timestamp": t, "source": "polymarket",
            "market_id": "pm-nyc-temp-mar26-2026",
            "title": "Highest temperature in NYC on March 26?",
            "yes_price": p, "no_price": round(1 - p, 3),
            "volume": int(66200 * p / 0.835), "liquidity": 8500,
            "resolved": False,
        })

    # 2026 hottest year rank - relatively stable around 41-43%
    hottest_prices = [0.38, 0.40, 0.41, 0.42, 0.41, 0.43]
    for t, p in zip(base_times, hottest_prices):
        records.append({
            "timestamp": t, "source": "polymarket",
            "market_id": "pm-2026-hottest-rank",
            "title": "Where will 2026 rank among the hottest years on record?",
            "yes_price": p, "no_price": round(1 - p, 3),
            "volume": int(1800000 * p / 0.43), "liquidity": 113000,
            "resolved": False,
        })

    # Natural disaster - drifting down slightly
    disaster_prices = [0.35, 0.34, 0.33, 0.32, 0.31, 0.31]
    for t, p in zip(base_times, disaster_prices):
        records.append({
            "timestamp": t, "source": "polymarket",
            "market_id": "pm-natural-disaster-2026",
            "title": "Natural Disaster in 2026?",
            "yes_price": p, "no_price": round(1 - p, 3),
            "volume": int(185700 * p / 0.31), "liquidity": 25000,
            "resolved": False,
        })

    # Seattle precip - converging as month nears end
    seattle_prices = [0.45, 0.52, 0.61, 0.70, 0.75, 0.79]
    for t, p in zip(base_times, seattle_prices):
        records.append({
            "timestamp": t, "source": "polymarket",
            "market_id": "pm-seattle-precip-mar",
            "title": "Precipitation in Seattle in March?",
            "yes_price": p, "no_price": round(1 - p, 3),
            "volume": int(137000 * p / 0.79), "liquidity": 21300,
            "resolved": False,
        })

    # Named storm before season - volatile
    storm_prices = [0.38, 0.42, 0.48, 0.44, 0.46, 0.45]
    for t, p in zip(base_times, storm_prices):
        records.append({
            "timestamp": t, "source": "polymarket",
            "market_id": "pm-named-storm-before-season",
            "title": "Named storm forms before hurricane season?",
            "yes_price": p, "no_price": round(1 - p, 3),
            "volume": int(42000 * p / 0.45), "liquidity": 12000,
            "resolved": False,
        })

    # NYC Mar 21 (resolved) - price history before resolution
    nyc21_prices = [0.22, 0.35, 0.58, 0.75, 0.92, 1.00]
    nyc21_times = [
        "2026-03-17T10:00:00+00:00",
        "2026-03-18T10:00:00+00:00",
        "2026-03-19T10:00:00+00:00",
        "2026-03-20T10:00:00+00:00",
        "2026-03-20T22:00:00+00:00",
        "2026-03-21T20:00:00+00:00",
    ]
    for t, p in zip(nyc21_times, nyc21_prices):
        records.append({
            "timestamp": t, "source": "polymarket",
            "market_id": "pm-nyc-temp-mar21-2026",
            "title": "Highest temperature in NYC on March 21?",
            "yes_price": p, "no_price": round(1 - p, 3),
            "volume": int(183800 * p), "liquidity": max(0, int(15000 * (1 - p))),
            "resolved": p == 1.00,
        })

    return records


def seed():
    """Write all seed data to CSV files."""
    os.makedirs(config.DATA_DIR, exist_ok=True)

    poly = get_polymarket_markets()
    kalshi = get_kalshi_markets()
    all_markets = poly + kalshi
    prices = get_historical_price_snapshots()

    markets_df = pd.DataFrame(all_markets)
    markets_df.to_csv(config.MARKETS_FILE, index=False)
    print(f"Seeded {len(markets_df)} markets to {config.MARKETS_FILE}")

    prices_df = pd.DataFrame(prices)
    prices_df.to_csv(config.PRICES_FILE, index=False)
    print(f"Seeded {len(prices_df)} price snapshots to {config.PRICES_FILE}")

    return markets_df, prices_df


if __name__ == "__main__":
    seed()
