#!/usr/bin/env python3
"""
Generate realistic seed data for the weather prediction market pipeline.

This provides demonstration data for testing the analysis pipeline
when live API access is unavailable. The data mimics real patterns
from Polymarket/Kalshi weather markets and Open-Meteo observations.
"""

import json
import random
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from storage import (
    append_market_snapshots,
    append_weather_actuals,
    append_resolved_markets,
)

random.seed(42)
np.random.seed(42)

# Realistic weather market templates based on actual Kalshi/Polymarket patterns
MARKET_TEMPLATES = [
    {
        "source": "kalshi",
        "base_question": "Will the high temperature in {city} be above {threshold}°F on {date}?",
        "category": "temperature_high",
    },
    {
        "source": "kalshi",
        "base_question": "Will the low temperature in {city} drop below {threshold}°F on {date}?",
        "category": "temperature_low",
    },
    {
        "source": "polymarket",
        "base_question": "Will {city} receive more than {threshold} inches of rain this week ({date})?",
        "category": "precipitation",
    },
    {
        "source": "kalshi",
        "base_question": "Will there be measurable snowfall in {city} on {date}?",
        "category": "snowfall",
    },
    {
        "source": "polymarket",
        "base_question": "Will a hurricane make landfall in the US by {date}?",
        "category": "hurricane",
    },
    {
        "source": "kalshi",
        "base_question": "Will wind speeds exceed {threshold} mph in {city} on {date}?",
        "category": "wind",
    },
]

CITIES = {
    "New York": {"avg_high_mar": 52, "avg_low_mar": 36, "rain_prob": 0.35},
    "Chicago": {"avg_high_mar": 47, "avg_low_mar": 30, "rain_prob": 0.30},
    "Miami": {"avg_high_mar": 80, "avg_low_mar": 66, "rain_prob": 0.25},
    "Los Angeles": {"avg_high_mar": 68, "avg_low_mar": 52, "rain_prob": 0.15},
    "Houston": {"avg_high_mar": 72, "avg_low_mar": 52, "rain_prob": 0.30},
    "Denver": {"avg_high_mar": 52, "avg_low_mar": 26, "rain_prob": 0.20},
    "Seattle": {"avg_high_mar": 53, "avg_low_mar": 39, "rain_prob": 0.45},
    "Phoenix": {"avg_high_mar": 78, "avg_low_mar": 52, "rain_prob": 0.10},
    "Boston": {"avg_high_mar": 46, "avg_low_mar": 31, "rain_prob": 0.35},
    "Atlanta": {"avg_high_mar": 64, "avg_low_mar": 42, "rain_prob": 0.30},
}

CITY_KEYS = {
    "New York": "new_york", "Chicago": "chicago", "Miami": "miami",
    "Los Angeles": "los_angeles", "Houston": "houston", "Denver": "denver",
    "Seattle": "seattle", "Phoenix": "phoenix", "Boston": "boston",
    "Atlanta": "atlanta",
}


def generate_market_snapshots(n_days=30, snapshots_per_day=2):
    """Generate rolling time series of market price snapshots."""
    markets = []
    base_date = datetime.now(timezone.utc) - timedelta(days=n_days)

    market_id_counter = 1000

    for day_offset in range(n_days):
        current_date = base_date + timedelta(days=day_offset)
        target_date = current_date + timedelta(days=random.randint(1, 7))
        target_str = target_date.strftime("%Y-%m-%d")

        for city_name, city_stats in CITIES.items():
            # Temperature high market
            threshold = city_stats["avg_high_mar"] + random.randint(-5, 10)
            actual_will_exceed = random.random() < 0.5 + (threshold - city_stats["avg_high_mar"]) * (-0.05)
            # Price reflects probability with some noise
            true_prob = max(0.05, min(0.95, 0.5 + (city_stats["avg_high_mar"] - threshold) * 0.05))
            market_noise = np.random.normal(0, 0.08)
            price = max(0.02, min(0.98, true_prob + market_noise))

            market_id = f"KALSHI-HIGH-{CITY_KEYS[city_name].upper()}-{market_id_counter}"
            is_resolved = day_offset < n_days - 7  # Older markets are resolved

            for snap in range(snapshots_per_day):
                snap_time = current_date + timedelta(hours=snap * 8 + random.randint(0, 3))
                # Price drifts slightly between snapshots
                snap_price = max(0.02, min(0.98, price + np.random.normal(0, 0.02)))

                markets.append({
                    "source": "kalshi",
                    "market_id": market_id,
                    "question": f"Will the high temperature in {city_name} be above {threshold}°F on {target_str}?",
                    "description": f"Resolves YES if the high temperature recorded at the official weather station in {city_name} exceeds {threshold}°F.",
                    "end_date": target_date.isoformat(),
                    "price_yes": round(snap_price, 4),
                    "volume": random.randint(5000, 500000),
                    "liquidity": random.randint(1000, 100000),
                    "resolved": is_resolved,
                    "resolution": ("Yes" if actual_will_exceed else "No") if is_resolved else None,
                    "fetch_timestamp": snap_time.isoformat(),
                    "raw_tags": "weather,temperature",
                })

            # Precipitation market (Polymarket style)
            rain_threshold = round(random.uniform(0.1, 2.0), 1)
            rain_prob = city_stats["rain_prob"] * (1 + np.random.normal(0, 0.3))
            rain_prob = max(0.05, min(0.95, rain_prob))
            rain_price = max(0.02, min(0.98, rain_prob + np.random.normal(0, 0.1)))
            actual_rained = random.random() < city_stats["rain_prob"]

            market_id_counter += 1
            precip_id = f"POLY-RAIN-{CITY_KEYS[city_name].upper()}-{market_id_counter}"

            for snap in range(snapshots_per_day):
                snap_time = current_date + timedelta(hours=snap * 8 + random.randint(0, 3))
                snap_price = max(0.02, min(0.98, rain_price + np.random.normal(0, 0.03)))

                markets.append({
                    "source": "polymarket",
                    "market_id": precip_id,
                    "question": f"Will {city_name} receive more than {rain_threshold} inches of rain this week ({target_str})?",
                    "description": "",
                    "end_date": target_date.isoformat(),
                    "price_yes": round(snap_price, 4),
                    "volume": random.randint(1000, 200000),
                    "liquidity": random.randint(500, 50000),
                    "resolved": is_resolved,
                    "resolution": ("Yes" if actual_rained else "No") if is_resolved else None,
                    "fetch_timestamp": snap_time.isoformat(),
                    "raw_tags": "weather,precipitation",
                })

            market_id_counter += 1

    return markets


def generate_weather_actuals(n_days=30):
    """Generate realistic weather observation data."""
    records = []
    base_date = datetime.now(timezone.utc) - timedelta(days=n_days)

    for day_offset in range(n_days):
        date_str = (base_date + timedelta(days=day_offset)).strftime("%Y-%m-%d")

        for city_name, stats in CITIES.items():
            # Add realistic day-to-day variability
            temp_anomaly = np.random.normal(0, 8)
            high = round(stats["avg_high_mar"] + temp_anomaly + random.uniform(-2, 2), 1)
            low = round(stats["avg_low_mar"] + temp_anomaly + random.uniform(-3, 1), 1)
            mean_temp = round((high + low) / 2, 1)

            did_rain = random.random() < stats["rain_prob"]
            precip = round(random.uniform(0.01, 1.5), 2) if did_rain else 0.0
            rain = precip
            snow = round(random.uniform(0.1, 4.0), 1) if (low < 34 and did_rain) else 0.0
            wind = round(random.uniform(5, 35), 1)

            records.append({
                "city": CITY_KEYS[city_name],
                "city_name": city_name,
                "date": date_str,
                "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
                "temperature_2m_max": high,
                "temperature_2m_min": low,
                "temperature_2m_mean": mean_temp,
                "precipitation_sum": precip,
                "snowfall_sum": snow,
                "windspeed_10m_max": wind,
                "rain_sum": rain,
            })

    return records


def generate_resolved_markets(market_snapshots):
    """Extract resolved markets from snapshots (latest snapshot per resolved market)."""
    resolved = []
    seen = set()

    # Sort by timestamp descending to get latest first
    sorted_markets = sorted(market_snapshots, key=lambda m: m["fetch_timestamp"], reverse=True)

    for m in sorted_markets:
        if m["resolved"] and m["market_id"] not in seen:
            seen.add(m["market_id"])
            entry = dict(m)
            entry["resolved_yes"] = m.get("resolution") == "Yes"
            resolved.append(entry)

    return resolved


def seed_all():
    """Generate and store all seed data."""
    print("Generating seed market snapshots (30 days, ~1200 snapshots)...")
    snapshots = generate_market_snapshots(n_days=30, snapshots_per_day=2)
    n = append_market_snapshots(snapshots)
    print(f"  Stored {n} market snapshots")

    print("Generating seed weather actuals (30 days, 10 cities)...")
    weather = generate_weather_actuals(n_days=30)
    n = append_weather_actuals(weather)
    print(f"  Stored {n} weather records")

    print("Extracting resolved markets...")
    resolved = generate_resolved_markets(snapshots)
    n = append_resolved_markets(resolved)
    print(f"  Stored {n} resolved market records")

    print("\nSeed data generation complete!")
    return len(snapshots), len(weather), len(resolved)


if __name__ == "__main__":
    seed_all()
