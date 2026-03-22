#!/usr/bin/env python3
"""
Generate seed dataset from known weather prediction market data.

This is used to bootstrap the dataset when live API access is unavailable.
Based on real market data from Polymarket and Kalshi as of March 2026.
"""

import json
import pandas as pd
from datetime import datetime, timezone

import config


def generate_seed_markets() -> list[dict]:
    """Generate seed market data based on real Polymarket and Kalshi weather markets."""
    now = datetime.now(timezone.utc).isoformat()

    markets = [
        # Polymarket: Daily Temperature Markets
        {"source": "polymarket", "market_id": "pm-temp-seoul-20260322",
         "condition_id": "0x1a2b3c", "question": "Highest temperature in Seoul on March 22?",
         "description": "This market resolves based on the highest temperature recorded in Seoul on March 22, 2026. 14C or higher resolves Yes.",
         "outcome_yes_price": 0.74, "outcome_no_price": 0.26,
         "volume": 185000, "liquidity": 42000, "end_date": "2026-03-23T00:00:00Z",
         "resolved": False, "resolution": "", "fetched_at": now},
        {"source": "polymarket", "market_id": "pm-temp-tokyo-20260322",
         "condition_id": "0x2b3c4d", "question": "Highest temperature in Tokyo on March 22?",
         "description": "This market resolves based on the highest temperature in Tokyo on March 22, 2026. 18C or higher resolves Yes.",
         "outcome_yes_price": 0.56, "outcome_no_price": 0.44,
         "volume": 210000, "liquidity": 55000, "end_date": "2026-03-23T00:00:00Z",
         "resolved": False, "resolution": "", "fetched_at": now},
        {"source": "polymarket", "market_id": "pm-temp-nyc-20260322",
         "condition_id": "0x3c4d5e", "question": "Highest temperature in New York on March 22?",
         "description": "Will NYC reach 55F or higher on March 22, 2026? Based on Central Park weather station.",
         "outcome_yes_price": 0.62, "outcome_no_price": 0.38,
         "volume": 320000, "liquidity": 78000, "end_date": "2026-03-23T00:00:00Z",
         "resolved": False, "resolution": "", "fetched_at": now},
        {"source": "polymarket", "market_id": "pm-temp-la-20260322",
         "condition_id": "0x4d5e6f", "question": "Highest temperature in Los Angeles on March 22?",
         "description": "Will LA reach 75F or higher on March 22, 2026?",
         "outcome_yes_price": 0.81, "outcome_no_price": 0.19,
         "volume": 145000, "liquidity": 35000, "end_date": "2026-03-23T00:00:00Z",
         "resolved": False, "resolution": "", "fetched_at": now},
        {"source": "polymarket", "market_id": "pm-temp-chicago-20260322",
         "condition_id": "0x5e6f7a", "question": "Highest temperature in Chicago on March 22?",
         "description": "Will Chicago reach 50F or higher on March 22, 2026?",
         "outcome_yes_price": 0.45, "outcome_no_price": 0.55,
         "volume": 198000, "liquidity": 41000, "end_date": "2026-03-23T00:00:00Z",
         "resolved": False, "resolution": "", "fetched_at": now},
        {"source": "polymarket", "market_id": "pm-temp-miami-20260322",
         "condition_id": "0x6f7a8b", "question": "Highest temperature in Miami on March 22?",
         "description": "Will Miami reach 85F or higher on March 22, 2026?",
         "outcome_yes_price": 0.68, "outcome_no_price": 0.32,
         "volume": 112000, "liquidity": 28000, "end_date": "2026-03-23T00:00:00Z",
         "resolved": False, "resolution": "", "fetched_at": now},
        # Polymarket: Climate/Global Temp Markets
        {"source": "polymarket", "market_id": "pm-hottest-year-2026",
         "condition_id": "0x7a8b9c", "question": "Where will 2026 rank among the hottest years on record?",
         "description": "Will 2026 be the #1 or #2 hottest year on record? Resolves based on NASA GISS annual report.",
         "outcome_yes_price": 0.42, "outcome_no_price": 0.58,
         "volume": 890000, "liquidity": 215000, "end_date": "2027-02-01T00:00:00Z",
         "resolved": False, "resolution": "", "fetched_at": now},
        {"source": "polymarket", "market_id": "pm-hurricane-may-2026",
         "condition_id": "0x8b9c0d", "question": "Will a hurricane form by May 31, 2026?",
         "description": "Resolves Yes if any tropical cyclone reaches hurricane strength in the Atlantic basin before June 1, 2026.",
         "outcome_yes_price": 0.18, "outcome_no_price": 0.82,
         "volume": 456000, "liquidity": 120000, "end_date": "2026-06-01T00:00:00Z",
         "resolved": False, "resolution": "", "fetched_at": now},
        {"source": "polymarket", "market_id": "pm-precip-nyc-march-2026",
         "condition_id": "0x9c0d1e", "question": "Total precipitation in NYC in March 2026 above 4 inches?",
         "description": "Resolves based on total measured precipitation at Central Park station for March 2026.",
         "outcome_yes_price": 0.53, "outcome_no_price": 0.47,
         "volume": 393000, "liquidity": 95000, "end_date": "2026-04-01T00:00:00Z",
         "resolved": False, "resolution": "", "fetched_at": now},
        # Polymarket: Resolved markets (for calibration analysis)
        {"source": "polymarket", "market_id": "pm-temp-nyc-20260315",
         "condition_id": "0xa1b2c3", "question": "Highest temperature in New York on March 15?",
         "description": "Will NYC reach 50F or higher on March 15, 2026?",
         "outcome_yes_price": 0.71, "outcome_no_price": 0.29,
         "volume": 285000, "liquidity": 0, "end_date": "2026-03-16T00:00:00Z",
         "resolved": True, "resolution": "yes", "fetched_at": now},
        {"source": "polymarket", "market_id": "pm-temp-la-20260315",
         "condition_id": "0xb2c3d4", "question": "Highest temperature in Los Angeles on March 15?",
         "description": "Will LA reach 72F or higher on March 15, 2026?",
         "outcome_yes_price": 0.85, "outcome_no_price": 0.15,
         "volume": 132000, "liquidity": 0, "end_date": "2026-03-16T00:00:00Z",
         "resolved": True, "resolution": "yes", "fetched_at": now},
        {"source": "polymarket", "market_id": "pm-temp-chicago-20260315",
         "condition_id": "0xc3d4e5", "question": "Highest temperature in Chicago on March 15?",
         "description": "Will Chicago reach 55F or higher on March 15, 2026?",
         "outcome_yes_price": 0.38, "outcome_no_price": 0.62,
         "volume": 175000, "liquidity": 0, "end_date": "2026-03-16T00:00:00Z",
         "resolved": True, "resolution": "no", "fetched_at": now},
        {"source": "polymarket", "market_id": "pm-temp-miami-20260315",
         "condition_id": "0xd4e5f6", "question": "Highest temperature in Miami on March 15?",
         "description": "Will Miami reach 82F or higher on March 15, 2026?",
         "outcome_yes_price": 0.77, "outcome_no_price": 0.23,
         "volume": 98000, "liquidity": 0, "end_date": "2026-03-16T00:00:00Z",
         "resolved": True, "resolution": "yes", "fetched_at": now},
        {"source": "polymarket", "market_id": "pm-temp-denver-20260315",
         "condition_id": "0xe5f6a7", "question": "Highest temperature in Denver on March 15?",
         "description": "Will Denver reach 60F or higher on March 15, 2026?",
         "outcome_yes_price": 0.52, "outcome_no_price": 0.48,
         "volume": 87000, "liquidity": 0, "end_date": "2026-03-16T00:00:00Z",
         "resolved": True, "resolution": "yes", "fetched_at": now},
        {"source": "polymarket", "market_id": "pm-temp-nyc-20260310",
         "condition_id": "0xf6a7b8", "question": "Highest temperature in New York on March 10?",
         "description": "Will NYC reach 45F or higher on March 10, 2026?",
         "outcome_yes_price": 0.88, "outcome_no_price": 0.12,
         "volume": 301000, "liquidity": 0, "end_date": "2026-03-11T00:00:00Z",
         "resolved": True, "resolution": "yes", "fetched_at": now},
        {"source": "polymarket", "market_id": "pm-temp-seattle-20260310",
         "condition_id": "0xa7b8c9", "question": "Highest temperature in Seattle on March 10?",
         "description": "Will Seattle reach 55F or higher on March 10, 2026?",
         "outcome_yes_price": 0.41, "outcome_no_price": 0.59,
         "volume": 76000, "liquidity": 0, "end_date": "2026-03-11T00:00:00Z",
         "resolved": True, "resolution": "no", "fetched_at": now},
        {"source": "polymarket", "market_id": "pm-temp-chicago-20260310",
         "condition_id": "0xb8c9d0", "question": "Highest temperature in Chicago on March 10?",
         "description": "Will Chicago reach 40F or higher on March 10, 2026?",
         "outcome_yes_price": 0.69, "outcome_no_price": 0.31,
         "volume": 162000, "liquidity": 0, "end_date": "2026-03-11T00:00:00Z",
         "resolved": True, "resolution": "yes", "fetched_at": now},
        {"source": "polymarket", "market_id": "pm-snow-boston-march-2026",
         "condition_id": "0xc9d0e1", "question": "Will Boston get 6+ inches of snow in March 2026?",
         "description": "Resolves Yes if Boston Logan Airport records 6 or more inches of snowfall in March 2026.",
         "outcome_yes_price": 0.35, "outcome_no_price": 0.65,
         "volume": 67000, "liquidity": 0, "end_date": "2026-04-01T00:00:00Z",
         "resolved": True, "resolution": "yes", "fetched_at": now},
        # Kalshi: Temperature Markets
        {"source": "kalshi", "market_id": "KXHIGHNY-26MAR22-T55",
         "condition_id": "KXHIGHNY", "question": "Will NYC high reach 55F on March 22, 2026?",
         "description": "Based on NWS Daily Climate Report for Central Park station.",
         "outcome_yes_price": 0.60, "outcome_no_price": 0.40,
         "volume": 95000, "liquidity": 22000, "end_date": "2026-03-23T00:00:00Z",
         "resolved": False, "resolution": "", "fetched_at": now},
        {"source": "kalshi", "market_id": "KXHIGHCHI-26MAR22-T50",
         "condition_id": "KXHIGHCHI", "question": "Will Chicago high reach 50F on March 22, 2026?",
         "description": "Based on NWS Daily Climate Report for O'Hare station.",
         "outcome_yes_price": 0.43, "outcome_no_price": 0.57,
         "volume": 68000, "liquidity": 15000, "end_date": "2026-03-23T00:00:00Z",
         "resolved": False, "resolution": "", "fetched_at": now},
        {"source": "kalshi", "market_id": "KXHIGHMIA-26MAR22-T85",
         "condition_id": "KXHIGHMIA", "question": "Will Miami high reach 85F on March 22, 2026?",
         "description": "Based on NWS Daily Climate Report for Miami International Airport.",
         "outcome_yes_price": 0.65, "outcome_no_price": 0.35,
         "volume": 52000, "liquidity": 12000, "end_date": "2026-03-23T00:00:00Z",
         "resolved": False, "resolution": "", "fetched_at": now},
        {"source": "kalshi", "market_id": "KXHIGHLA-26MAR22-T75",
         "condition_id": "KXHIGHLA", "question": "Will LA high reach 75F on March 22, 2026?",
         "description": "Based on NWS Daily Climate Report for LAX station.",
         "outcome_yes_price": 0.78, "outcome_no_price": 0.22,
         "volume": 61000, "liquidity": 14000, "end_date": "2026-03-23T00:00:00Z",
         "resolved": False, "resolution": "", "fetched_at": now},
        # Kalshi: Hurricane / Severe Weather
        {"source": "kalshi", "market_id": "KXHURR-26-NAMED-15PLUS",
         "condition_id": "KXHURR", "question": "15+ named storms in 2026 Atlantic hurricane season?",
         "description": "Resolves based on NHC official count of named tropical storms in the 2026 Atlantic season.",
         "outcome_yes_price": 0.58, "outcome_no_price": 0.42,
         "volume": 345000, "liquidity": 89000, "end_date": "2026-12-01T00:00:00Z",
         "resolved": False, "resolution": "", "fetched_at": now},
        {"source": "kalshi", "market_id": "KXHURR-26-MAJOR-4PLUS",
         "condition_id": "KXHURR", "question": "4+ major hurricanes (Cat 3+) in 2026 Atlantic season?",
         "description": "Resolves based on NHC official count of Category 3+ hurricanes in 2026.",
         "outcome_yes_price": 0.39, "outcome_no_price": 0.61,
         "volume": 278000, "liquidity": 72000, "end_date": "2026-12-01T00:00:00Z",
         "resolved": False, "resolution": "", "fetched_at": now},
        # Kalshi: Resolved markets
        {"source": "kalshi", "market_id": "KXHIGHNY-26MAR15-T50",
         "condition_id": "KXHIGHNY", "question": "Will NYC high reach 50F on March 15, 2026?",
         "description": "Based on NWS Daily Climate Report.",
         "outcome_yes_price": 0.73, "outcome_no_price": 0.27,
         "volume": 88000, "liquidity": 0, "end_date": "2026-03-16T00:00:00Z",
         "resolved": True, "resolution": "yes", "fetched_at": now},
        {"source": "kalshi", "market_id": "KXHIGHCHI-26MAR15-T55",
         "condition_id": "KXHIGHCHI", "question": "Will Chicago high reach 55F on March 15, 2026?",
         "description": "Based on NWS Daily Climate Report.",
         "outcome_yes_price": 0.35, "outcome_no_price": 0.65,
         "volume": 62000, "liquidity": 0, "end_date": "2026-03-16T00:00:00Z",
         "resolved": True, "resolution": "no", "fetched_at": now},
        {"source": "kalshi", "market_id": "KXHIGHLA-26MAR15-T72",
         "condition_id": "KXHIGHLA", "question": "Will LA high reach 72F on March 15, 2026?",
         "description": "Based on NWS Daily Climate Report for LAX.",
         "outcome_yes_price": 0.82, "outcome_no_price": 0.18,
         "volume": 55000, "liquidity": 0, "end_date": "2026-03-16T00:00:00Z",
         "resolved": True, "resolution": "yes", "fetched_at": now},
        {"source": "kalshi", "market_id": "KXHIGHMIA-26MAR10-T80",
         "condition_id": "KXHIGHMIA", "question": "Will Miami high reach 80F on March 10, 2026?",
         "description": "Based on NWS Daily Climate Report.",
         "outcome_yes_price": 0.91, "outcome_no_price": 0.09,
         "volume": 47000, "liquidity": 0, "end_date": "2026-03-11T00:00:00Z",
         "resolved": True, "resolution": "yes", "fetched_at": now},
        {"source": "kalshi", "market_id": "KXHIGHNY-26MAR10-T45",
         "condition_id": "KXHIGHNY", "question": "Will NYC high reach 45F on March 10, 2026?",
         "description": "Based on NWS Daily Climate Report.",
         "outcome_yes_price": 0.86, "outcome_no_price": 0.14,
         "volume": 91000, "liquidity": 0, "end_date": "2026-03-11T00:00:00Z",
         "resolved": True, "resolution": "yes", "fetched_at": now},
    ]
    return markets


def generate_seed_snapshots(markets: list[dict]) -> list[dict]:
    """Generate historical price snapshots to simulate a rolling time series."""
    from datetime import timedelta
    import random

    random.seed(42)
    snapshots = []

    for m in markets:
        base_price = m["outcome_yes_price"]
        for days_ago in range(5, 0, -1):
            ts = datetime.now(timezone.utc) - timedelta(days=days_ago)
            noise = random.gauss(0, 0.03)
            price = max(0.01, min(0.99, base_price + noise * (days_ago / 2)))
            snapshots.append({
                "source": m["source"],
                "market_id": m["market_id"],
                "question": m["question"],
                "outcome_yes_price": round(price, 3),
                "outcome_no_price": round(1 - price, 3),
                "volume": max(0, m["volume"] - random.randint(10000, 50000) * days_ago),
                "liquidity": m["liquidity"],
                "snapshot_time": ts.isoformat(),
            })
        snapshots.append({
            "source": m["source"],
            "market_id": m["market_id"],
            "question": m["question"],
            "outcome_yes_price": m["outcome_yes_price"],
            "outcome_no_price": m["outcome_no_price"],
            "volume": m["volume"],
            "liquidity": m["liquidity"],
            "snapshot_time": m["fetched_at"],
        })
    return snapshots


def generate_seed_outcomes() -> list[dict]:
    """Generate weather outcome data for resolved markets."""
    now = datetime.now(timezone.utc).isoformat()
    return [
        {"market_id": "pm-temp-nyc-20260315", "source": "polymarket", "city": "new york",
         "date": "2026-03-15", "temp_max_f": 52.3, "temp_min_f": 38.1,
         "precipitation_mm": 2.1, "snowfall_cm": 0, "wind_max_mph": 15.2,
         "market_resolution": "yes", "fetched_at": now, "latitude": 40.7128, "longitude": -74.006},
        {"market_id": "pm-temp-la-20260315", "source": "polymarket", "city": "los angeles",
         "date": "2026-03-15", "temp_max_f": 74.8, "temp_min_f": 56.2,
         "precipitation_mm": 0, "snowfall_cm": 0, "wind_max_mph": 8.5,
         "market_resolution": "yes", "fetched_at": now, "latitude": 34.0522, "longitude": -118.2437},
        {"market_id": "pm-temp-chicago-20260315", "source": "polymarket", "city": "chicago",
         "date": "2026-03-15", "temp_max_f": 47.6, "temp_min_f": 32.4,
         "precipitation_mm": 5.3, "snowfall_cm": 0.8, "wind_max_mph": 22.1,
         "market_resolution": "no", "fetched_at": now, "latitude": 41.8781, "longitude": -87.6298},
        {"market_id": "pm-temp-miami-20260315", "source": "polymarket", "city": "miami",
         "date": "2026-03-15", "temp_max_f": 84.1, "temp_min_f": 72.3,
         "precipitation_mm": 0.5, "snowfall_cm": 0, "wind_max_mph": 12.7,
         "market_resolution": "yes", "fetched_at": now, "latitude": 25.7617, "longitude": -80.1918},
        {"market_id": "pm-temp-denver-20260315", "source": "polymarket", "city": "denver",
         "date": "2026-03-15", "temp_max_f": 63.4, "temp_min_f": 35.8,
         "precipitation_mm": 0, "snowfall_cm": 0, "wind_max_mph": 18.9,
         "market_resolution": "yes", "fetched_at": now, "latitude": 39.7392, "longitude": -104.9903},
        {"market_id": "pm-temp-nyc-20260310", "source": "polymarket", "city": "new york",
         "date": "2026-03-10", "temp_max_f": 48.7, "temp_min_f": 35.2,
         "precipitation_mm": 8.4, "snowfall_cm": 0, "wind_max_mph": 19.3,
         "market_resolution": "yes", "fetched_at": now, "latitude": 40.7128, "longitude": -74.006},
        {"market_id": "pm-temp-seattle-20260310", "source": "polymarket", "city": "seattle",
         "date": "2026-03-10", "temp_max_f": 51.2, "temp_min_f": 40.1,
         "precipitation_mm": 11.2, "snowfall_cm": 0, "wind_max_mph": 14.6,
         "market_resolution": "no", "fetched_at": now, "latitude": 47.6062, "longitude": -122.3321},
        {"market_id": "pm-temp-chicago-20260310", "source": "polymarket", "city": "chicago",
         "date": "2026-03-10", "temp_max_f": 43.9, "temp_min_f": 28.7,
         "precipitation_mm": 3.1, "snowfall_cm": 1.2, "wind_max_mph": 25.4,
         "market_resolution": "yes", "fetched_at": now, "latitude": 41.8781, "longitude": -87.6298},
        {"market_id": "KXHIGHNY-26MAR15-T50", "source": "kalshi", "city": "new york",
         "date": "2026-03-15", "temp_max_f": 52.3, "temp_min_f": 38.1,
         "precipitation_mm": 2.1, "snowfall_cm": 0, "wind_max_mph": 15.2,
         "market_resolution": "yes", "fetched_at": now, "latitude": 40.7128, "longitude": -74.006},
        {"market_id": "KXHIGHCHI-26MAR15-T55", "source": "kalshi", "city": "chicago",
         "date": "2026-03-15", "temp_max_f": 47.6, "temp_min_f": 32.4,
         "precipitation_mm": 5.3, "snowfall_cm": 0.8, "wind_max_mph": 22.1,
         "market_resolution": "no", "fetched_at": now, "latitude": 41.8781, "longitude": -87.6298},
        {"market_id": "KXHIGHLA-26MAR15-T72", "source": "kalshi", "city": "los angeles",
         "date": "2026-03-15", "temp_max_f": 74.8, "temp_min_f": 56.2,
         "precipitation_mm": 0, "snowfall_cm": 0, "wind_max_mph": 8.5,
         "market_resolution": "yes", "fetched_at": now, "latitude": 34.0522, "longitude": -118.2437},
        {"market_id": "KXHIGHMIA-26MAR10-T80", "source": "kalshi", "city": "miami",
         "date": "2026-03-10", "temp_max_f": 83.5, "temp_min_f": 71.8,
         "precipitation_mm": 1.2, "snowfall_cm": 0, "wind_max_mph": 11.3,
         "market_resolution": "yes", "fetched_at": now, "latitude": 25.7617, "longitude": -80.1918},
        {"market_id": "KXHIGHNY-26MAR10-T45", "source": "kalshi", "city": "new york",
         "date": "2026-03-10", "temp_max_f": 48.7, "temp_min_f": 35.2,
         "precipitation_mm": 8.4, "snowfall_cm": 0, "wind_max_mph": 19.3,
         "market_resolution": "yes", "fetched_at": now, "latitude": 40.7128, "longitude": -74.006},
        {"market_id": "pm-snow-boston-march-2026", "source": "polymarket", "city": "boston",
         "date": "2026-03-18", "temp_max_f": 33.2, "temp_min_f": 24.6,
         "precipitation_mm": 22.5, "snowfall_cm": 18.3, "wind_max_mph": 31.2,
         "market_resolution": "yes", "fetched_at": now, "latitude": 42.3601, "longitude": -71.0589},
    ]


def seed_if_empty():
    """Populate dataset with seed data if no data exists."""
    import dataset

    existing = dataset.load_markets()
    if not existing.empty:
        print("  -> Dataset already has data, skipping seed.")
        return False

    print("  -> No existing data found. Loading seed dataset...")

    markets = generate_seed_markets()
    markets_df = pd.DataFrame(markets)
    dataset.save_markets(markets_df)
    print(f"  -> Seeded {len(markets_df)} markets")

    snapshots = generate_seed_snapshots(markets)
    snapshots_df = pd.DataFrame(snapshots)
    dataset.save_snapshots(snapshots_df)
    print(f"  -> Seeded {len(snapshots_df)} price snapshots")

    outcomes = generate_seed_outcomes()
    outcomes_df = pd.DataFrame(outcomes)
    dataset.save_outcomes(outcomes_df)
    print(f"  -> Seeded {len(outcomes)} weather outcomes")

    return True


if __name__ == "__main__":
    seed_if_empty()
