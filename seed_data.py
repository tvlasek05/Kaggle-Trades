#!/usr/bin/env python3
"""
Generate realistic seed data for the weather prediction market tracker.
This provides initial data for the pipeline to operate on.
When run in an environment with network access, live data will supplement this.
"""

import json
import os
import random
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

random.seed(42)
np.random.seed(42)


def generate_seed_markets():
    """Generate realistic weather prediction market data."""
    now = datetime.now(timezone.utc)
    markets = []

    # --- Active (open) markets ---
    active_markets = [
        {
            "source": "polymarket", "market_id": "poly-nyc-heat-apr-2026",
            "slug": "nyc-90f-april-2026",
            "question": "Will New York City reach 90°F in April 2026?",
            "description": "Resolves Yes if any official NWS station in NYC records 90°F+ in April 2026",
            "end_date": "2026-04-30T23:59:00Z", "resolved": False, "resolution": "",
            "outcome_prices": {"Yes": 0.12, "No": 0.89}, "volume": 45230, "liquidity": 12400,
        },
        {
            "source": "polymarket", "market_id": "poly-hurricane-cat3-2026",
            "slug": "cat3-hurricane-atlantic-2026",
            "question": "Will a Category 3+ hurricane make US landfall in 2026?",
            "description": "Resolves Yes if NHC classifies any 2026 Atlantic hurricane as Cat 3+ at US landfall",
            "end_date": "2026-11-30T23:59:00Z", "resolved": False, "resolution": "",
            "outcome_prices": {"Yes": 0.42, "No": 0.59}, "volume": 189500, "liquidity": 67000,
        },
        {
            "source": "polymarket", "market_id": "poly-la-rain-apr-2026",
            "slug": "la-rain-above-normal-april-2026",
            "question": "Will Los Angeles April 2026 rainfall exceed historical average?",
            "description": "Resolves Yes if total LAX rain gauge records above 0.8 inches for April 2026",
            "end_date": "2026-04-30T23:59:00Z", "resolved": False, "resolution": "",
            "outcome_prices": {"Yes": 0.31, "No": 0.70}, "volume": 23100, "liquidity": 8900,
        },
        {
            "source": "kalshi", "market_id": "KXHIGHNY-26MAR28-T75",
            "slug": "KXHIGHNY-26MAR28-T75",
            "question": "Will NYC high temperature reach 75°F on March 28, 2026?",
            "description": "NYC daily high temperature market",
            "end_date": "2026-03-28T23:59:00Z", "resolved": False, "resolution": "",
            "outcome_prices": {"Yes": 0.08, "No": 0.93}, "volume": 8750, "liquidity": 3200,
        },
        {
            "source": "kalshi", "market_id": "KXHIGHCHI-26MAR28-T60",
            "slug": "KXHIGHCHI-26MAR28-T60",
            "question": "Will Chicago high temperature reach 60°F on March 28, 2026?",
            "description": "Chicago daily high temperature market",
            "end_date": "2026-03-28T23:59:00Z", "resolved": False, "resolution": "",
            "outcome_prices": {"Yes": 0.35, "No": 0.66}, "volume": 5420, "liquidity": 2100,
        },
        {
            "source": "kalshi", "market_id": "KXHIGHMIA-26MAR28-T85",
            "slug": "KXHIGHMIA-26MAR28-T85",
            "question": "Will Miami high temperature reach 85°F on March 28, 2026?",
            "description": "Miami daily high temperature market",
            "end_date": "2026-03-28T23:59:00Z", "resolved": False, "resolution": "",
            "outcome_prices": {"Yes": 0.62, "No": 0.39}, "volume": 6100, "liquidity": 2800,
        },
        {
            "source": "kalshi", "market_id": "KXSNOWNY-26MAR-T1",
            "slug": "KXSNOWNY-26MAR-T1",
            "question": "Will NYC get 1+ inch of snow in March 2026?",
            "description": "NYC monthly snowfall market",
            "end_date": "2026-03-31T23:59:00Z", "resolved": False, "resolution": "",
            "outcome_prices": {"Yes": 0.55, "No": 0.46}, "volume": 11200, "liquidity": 4500,
        },
        {
            "source": "polymarket", "market_id": "poly-el-nino-2026",
            "slug": "el-nino-2026-summer",
            "question": "Will NOAA declare El Niño conditions by summer 2026?",
            "description": "Resolves Yes if NOAA issues an El Niño advisory before September 1, 2026",
            "end_date": "2026-09-01T00:00:00Z", "resolved": False, "resolution": "",
            "outcome_prices": {"Yes": 0.28, "No": 0.73}, "volume": 78900, "liquidity": 31000,
        },
        {
            "source": "polymarket", "market_id": "poly-hottest-summer-2026",
            "slug": "2026-hottest-summer-record",
            "question": "Will 2026 be the hottest summer on record globally?",
            "description": "Based on NOAA global surface temperature anomaly for June-August 2026",
            "end_date": "2026-09-30T23:59:00Z", "resolved": False, "resolution": "",
            "outcome_prices": {"Yes": 0.38, "No": 0.63}, "volume": 156000, "liquidity": 52000,
        },
        {
            "source": "kalshi", "market_id": "KXHIGHLA-26MAR28-T80",
            "slug": "KXHIGHLA-26MAR28-T80",
            "question": "Will Los Angeles high temperature reach 80°F on March 28, 2026?",
            "description": "LA daily high temperature market",
            "end_date": "2026-03-28T23:59:00Z", "resolved": False, "resolution": "",
            "outcome_prices": {"Yes": 0.45, "No": 0.56}, "volume": 4300, "liquidity": 1800,
        },
    ]

    # --- Resolved markets (for calibration analysis) ---
    resolved_markets = [
        {
            "source": "kalshi", "market_id": "KXHIGHNY-26MAR20-T55",
            "slug": "KXHIGHNY-26MAR20-T55",
            "question": "Will NYC high temperature reach 55°F on March 20, 2026?",
            "end_date": "2026-03-20T23:59:00Z", "resolved": True, "resolution": "yes",
            "final_price": 0.72, "actual_outcome": 1,  # It did reach 55
        },
        {
            "source": "kalshi", "market_id": "KXHIGHNY-26MAR20-T65",
            "slug": "KXHIGHNY-26MAR20-T65",
            "question": "Will NYC high temperature reach 65°F on March 20, 2026?",
            "end_date": "2026-03-20T23:59:00Z", "resolved": True, "resolution": "no",
            "final_price": 0.25, "actual_outcome": 0,  # It did NOT reach 65
        },
        {
            "source": "kalshi", "market_id": "KXHIGHCHI-26MAR20-T50",
            "slug": "KXHIGHCHI-26MAR20-T50",
            "question": "Will Chicago high temperature reach 50°F on March 20, 2026?",
            "end_date": "2026-03-20T23:59:00Z", "resolved": True, "resolution": "yes",
            "final_price": 0.58, "actual_outcome": 1,
        },
        {
            "source": "kalshi", "market_id": "KXHIGHMIA-26MAR20-T80",
            "slug": "KXHIGHMIA-26MAR20-T80",
            "question": "Will Miami high temperature reach 80°F on March 20, 2026?",
            "end_date": "2026-03-20T23:59:00Z", "resolved": True, "resolution": "yes",
            "final_price": 0.85, "actual_outcome": 1,
        },
        {
            "source": "kalshi", "market_id": "KXHIGHLA-26MAR20-T75",
            "slug": "KXHIGHLA-26MAR20-T75",
            "question": "Will Los Angeles high temperature reach 75°F on March 20, 2026?",
            "end_date": "2026-03-20T23:59:00Z", "resolved": True, "resolution": "no",
            "final_price": 0.40, "actual_outcome": 0,
        },
        {
            "source": "polymarket", "market_id": "poly-feb-snow-nyc-2026",
            "slug": "nyc-snowfall-feb-2026",
            "question": "Will NYC get 6+ inches of snow in any single storm in February 2026?",
            "end_date": "2026-02-28T23:59:00Z", "resolved": True, "resolution": "yes",
            "final_price": 0.55, "actual_outcome": 1,
        },
        {
            "source": "polymarket", "market_id": "poly-warmest-feb-2026",
            "slug": "warmest-february-record-2026",
            "question": "Will February 2026 be the warmest February on record globally?",
            "end_date": "2026-03-15T00:00:00Z", "resolved": True, "resolution": "no",
            "final_price": 0.30, "actual_outcome": 0,
        },
        {
            "source": "kalshi", "market_id": "KXHIGHNY-26MAR15-T60",
            "slug": "KXHIGHNY-26MAR15-T60",
            "question": "Will NYC high temperature reach 60°F on March 15, 2026?",
            "end_date": "2026-03-15T23:59:00Z", "resolved": True, "resolution": "no",
            "final_price": 0.35, "actual_outcome": 0,
        },
        {
            "source": "kalshi", "market_id": "KXHIGHNY-26MAR15-T50",
            "slug": "KXHIGHNY-26MAR15-T50",
            "question": "Will NYC high temperature reach 50°F on March 15, 2026?",
            "end_date": "2026-03-15T23:59:00Z", "resolved": True, "resolution": "yes",
            "final_price": 0.80, "actual_outcome": 1,
        },
        {
            "source": "kalshi", "market_id": "KXHIGHCHI-26MAR15-T45",
            "slug": "KXHIGHCHI-26MAR15-T45",
            "question": "Will Chicago high temperature reach 45°F on March 15, 2026?",
            "end_date": "2026-03-15T23:59:00Z", "resolved": True, "resolution": "yes",
            "final_price": 0.65, "actual_outcome": 1,
        },
        {
            "source": "kalshi", "market_id": "KXHIGHMIA-26MAR15-T82",
            "slug": "KXHIGHMIA-26MAR15-T82",
            "question": "Will Miami high temperature reach 82°F on March 15, 2026?",
            "end_date": "2026-03-15T23:59:00Z", "resolved": True, "resolution": "yes",
            "final_price": 0.70, "actual_outcome": 1,
        },
        {
            "source": "kalshi", "market_id": "KXHIGHLA-26MAR15-T70",
            "slug": "KXHIGHLA-26MAR15-T70",
            "question": "Will Los Angeles high temperature reach 70°F on March 15, 2026?",
            "end_date": "2026-03-15T23:59:00Z", "resolved": True, "resolution": "yes",
            "final_price": 0.75, "actual_outcome": 1,
        },
        # Some poorly calibrated examples
        {
            "source": "polymarket", "market_id": "poly-chicago-blizzard-feb-2026",
            "slug": "chicago-blizzard-feb-2026",
            "question": "Will Chicago experience a blizzard (8+ inches) in February 2026?",
            "end_date": "2026-02-28T23:59:00Z", "resolved": True, "resolution": "yes",
            "final_price": 0.18, "actual_outcome": 1,  # Market was wrong - underpriced
        },
        {
            "source": "polymarket", "market_id": "poly-miami-freeze-feb-2026",
            "slug": "miami-freeze-feb-2026",
            "question": "Will Miami experience freezing temperatures in February 2026?",
            "end_date": "2026-02-28T23:59:00Z", "resolved": True, "resolution": "no",
            "final_price": 0.05, "actual_outcome": 0,
        },
        {
            "source": "kalshi", "market_id": "KXHIGHNY-26MAR10-T55",
            "slug": "KXHIGHNY-26MAR10-T55",
            "question": "Will NYC high temperature reach 55°F on March 10, 2026?",
            "end_date": "2026-03-10T23:59:00Z", "resolved": True, "resolution": "no",
            "final_price": 0.62, "actual_outcome": 0,  # Market overpriced
        },
        {
            "source": "kalshi", "market_id": "KXHIGHNY-26MAR10-T45",
            "slug": "KXHIGHNY-26MAR10-T45",
            "question": "Will NYC high temperature reach 45°F on March 10, 2026?",
            "end_date": "2026-03-10T23:59:00Z", "resolved": True, "resolution": "yes",
            "final_price": 0.88, "actual_outcome": 1,
        },
    ]

    return active_markets, resolved_markets


def generate_price_history(active_markets, resolved_markets):
    """Generate rolling time series of prices for each market."""
    rows = []
    now = datetime.now(timezone.utc)

    # Active markets: generate 7 days of price history with realistic drift
    for m in active_markets:
        yes_price = m["outcome_prices"]["Yes"]
        no_price = m["outcome_prices"]["No"]
        for days_ago in range(6, -1, -1):
            ts = (now - timedelta(days=days_ago)).isoformat()
            # Add random walk noise
            drift = np.random.normal(0, 0.02)
            hist_yes = max(0.01, min(0.99, yes_price + drift * (days_ago / 3)))
            hist_no = max(0.01, min(0.99, 1.0 - hist_yes + np.random.normal(0, 0.01)))

            rows.append({
                "timestamp": ts, "source": m["source"], "market_id": m["market_id"],
                "question": m["question"], "outcome": "Yes",
                "price": round(hist_yes, 4), "volume": int(m["volume"] * (1 - days_ago * 0.08)),
                "resolved": False, "resolution": "",
            })
            rows.append({
                "timestamp": ts, "source": m["source"], "market_id": m["market_id"],
                "question": m["question"], "outcome": "No",
                "price": round(hist_no, 4), "volume": int(m["volume"] * (1 - days_ago * 0.08)),
                "resolved": False, "resolution": "",
            })

    # Resolved markets: generate price history leading up to resolution
    for m in resolved_markets:
        end = datetime.fromisoformat(m["end_date"].replace("Z", "+00:00"))
        final = m["final_price"]
        # Generate 10 days of history leading up to resolution
        for days_before in range(9, -1, -1):
            ts = (end - timedelta(days=days_before)).isoformat()
            # Price converges toward final as resolution approaches
            convergence = 1 - (days_before / 10)
            base_price = 0.5 + (final - 0.5) * convergence
            noise = np.random.normal(0, 0.03 * (1 - convergence))
            hist_yes = max(0.01, min(0.99, base_price + noise))

            rows.append({
                "timestamp": ts, "source": m["source"], "market_id": m["market_id"],
                "question": m["question"], "outcome": "Yes",
                "price": round(hist_yes, 4),
                "volume": int(random.randint(500, 15000)),
                "resolved": True, "resolution": m["resolution"],
            })
            rows.append({
                "timestamp": ts, "source": m["source"], "market_id": m["market_id"],
                "question": m["question"], "outcome": "No",
                "price": round(1 - hist_yes + np.random.normal(0, 0.01), 4),
                "volume": int(random.randint(500, 15000)),
                "resolved": True, "resolution": "no" if m["resolution"] == "yes" else "yes",
            })

    return pd.DataFrame(rows)


def generate_weather_actuals():
    """Generate realistic weather actual data for tracked cities."""
    rows = []
    now = datetime.now(timezone.utc)

    # March typical temperatures (°F) and conditions by city
    city_baselines = {
        "new_york": {"temp_max": 52, "temp_min": 36, "precip": 0.15, "snow": 0.1},
        "los_angeles": {"temp_max": 68, "temp_min": 50, "precip": 0.08, "snow": 0.0},
        "chicago": {"temp_max": 47, "temp_min": 30, "precip": 0.10, "snow": 0.2},
        "miami": {"temp_max": 81, "temp_min": 67, "precip": 0.12, "snow": 0.0},
        "houston": {"temp_max": 73, "temp_min": 54, "precip": 0.13, "snow": 0.0},
        "phoenix": {"temp_max": 78, "temp_min": 52, "precip": 0.03, "snow": 0.0},
        "boston": {"temp_max": 48, "temp_min": 33, "precip": 0.14, "snow": 0.15},
        "denver": {"temp_max": 55, "temp_min": 28, "precip": 0.05, "snow": 0.3},
        "seattle": {"temp_max": 53, "temp_min": 39, "precip": 0.18, "snow": 0.02},
        "atlanta": {"temp_max": 65, "temp_min": 44, "precip": 0.16, "snow": 0.0},
        "dallas": {"temp_max": 68, "temp_min": 47, "precip": 0.12, "snow": 0.0},
        "san_francisco": {"temp_max": 62, "temp_min": 48, "precip": 0.10, "snow": 0.0},
        "washington_dc": {"temp_max": 57, "temp_min": 38, "precip": 0.13, "snow": 0.05},
        "minneapolis": {"temp_max": 42, "temp_min": 24, "precip": 0.08, "snow": 0.25},
    }

    coords = {
        "new_york": (40.7128, -74.0060), "los_angeles": (34.0522, -118.2437),
        "chicago": (41.8781, -87.6298), "miami": (25.7617, -80.1918),
        "houston": (29.7604, -95.3698), "phoenix": (33.4484, -112.0740),
        "boston": (42.3601, -71.0589), "denver": (39.7392, -104.9903),
        "seattle": (47.6062, -122.3321), "atlanta": (33.7490, -84.3880),
        "dallas": (32.7767, -96.7970), "san_francisco": (37.7749, -122.4194),
        "washington_dc": (38.9072, -77.0369), "minneapolis": (44.9778, -93.2650),
    }

    for days_ago in range(14, -1, -1):
        date = (now - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        for city, baseline in city_baselines.items():
            lat, lon = coords[city]
            # Add daily variation
            temp_var = np.random.normal(0, 5)
            rows.append({
                "date": date,
                "city": city,
                "lat": lat,
                "lon": lon,
                "temp_max_f": round(baseline["temp_max"] + temp_var, 1),
                "temp_min_f": round(baseline["temp_min"] + temp_var * 0.7, 1),
                "temp_mean_f": round((baseline["temp_max"] + baseline["temp_min"]) / 2 + temp_var * 0.85, 1),
                "precip_inches": round(max(0, baseline["precip"] + np.random.normal(0, 0.1)), 2),
                "snow_inches": round(max(0, baseline["snow"] + np.random.normal(0, 0.15)), 2),
                "rain_inches": round(max(0, baseline["precip"] + np.random.normal(0, 0.08)), 2),
                "wind_max_mph": round(max(0, 12 + np.random.normal(0, 5)), 1),
                "weather_code": random.choice([0, 1, 2, 3, 45, 51, 61, 71, 80]),
            })

    return pd.DataFrame(rows)


def seed():
    """Generate and save all seed data."""
    os.makedirs(DATA_DIR, exist_ok=True)

    print("Generating seed market data...")
    active, resolved = generate_seed_markets()

    print("Generating price time series...")
    prices_df = generate_price_history(active, resolved)
    prices_df.to_csv(os.path.join(DATA_DIR, "market_prices.csv"), index=False)
    print(f"  -> {len(prices_df)} price snapshots for {prices_df['market_id'].nunique()} markets")

    print("Generating market metadata...")
    metadata = {}
    for m in active + [{**r, "outcome_prices": {"Yes": r["final_price"], "No": 1-r["final_price"]},
                         "description": r["question"], "liquidity": 0} for r in resolved]:
        metadata[m["market_id"]] = {
            "source": m["source"], "question": m["question"],
            "description": m.get("description", ""), "slug": m["slug"],
            "end_date": m["end_date"], "category": "weather",
        }
    with open(os.path.join(DATA_DIR, "markets_metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2)

    print("Generating weather actuals...")
    actuals_df = generate_weather_actuals()
    actuals_df.to_csv(os.path.join(DATA_DIR, "weather_actuals.csv"), index=False)
    print(f"  -> {len(actuals_df)} weather records for {actuals_df['city'].nunique()} cities")

    print("\nSeed data generation complete!")
    return prices_df, actuals_df


if __name__ == "__main__":
    seed()
