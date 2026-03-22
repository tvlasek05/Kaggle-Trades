#!/usr/bin/env python3
"""
Generate realistic seed data for demonstration and testing.
Simulates Kalshi weather market snapshots and Open-Meteo weather actuals.
"""

import json
import os
import random
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

import config

random.seed(42)
np.random.seed(42)


def generate_weather_actuals(start_date, days=30):
    """Generate realistic weather data for all cities."""
    rows = []
    # Typical March temps by city (°F)
    city_baselines = {
        "NYC": {"high": 52, "low": 36, "precip_prob": 0.3},
        "CHI": {"high": 48, "low": 32, "precip_prob": 0.3},
        "LAX": {"high": 68, "low": 52, "precip_prob": 0.1},
        "MIA": {"high": 82, "low": 68, "precip_prob": 0.2},
    }

    for city, baseline in city_baselines.items():
        for d in range(days):
            date = start_date + timedelta(days=d)
            high = baseline["high"] + np.random.normal(0, 5)
            low = baseline["low"] + np.random.normal(0, 4)
            precip = max(0, np.random.exponential(3)) if random.random() < baseline["precip_prob"] else 0

            rows.append({
                "date": date.strftime("%Y-%m-%d"),
                "city": city,
                "temp_max_f": round(high, 1),
                "temp_min_f": round(low, 1),
                "precipitation_mm": round(precip, 1),
                "snowfall_mm": round(max(0, precip * 0.3) if high < 35 else 0, 1),
                "weathercode": random.choice([0, 1, 2, 3, 51, 61, 71]) if precip > 0 else random.choice([0, 1, 2]),
                "windspeed_max_kmh": round(np.random.gamma(3, 5), 1),
            })

    return pd.DataFrame(rows)


def generate_market_snapshots(weather_df, n_snapshots=3):
    """Generate Kalshi-style temperature market snapshots based on weather actuals."""
    rows = []
    series_map = {
        "NYC": "KXHIGHNY",
        "CHI": "KXHIGHCHI",
        "LAX": "KXHIGHLAX",
        "MIA": "KXHIGHMI",
    }

    month_abbrevs = {1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
                     7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC"}

    for _, wx in weather_df.iterrows():
        city = wx["city"]
        series = series_map.get(city)
        if not series:
            continue

        dt = datetime.strptime(wx["date"], "%Y-%m-%d")
        date_code = f"{dt.day:02d}{month_abbrevs[dt.month]}{dt.year % 100:02d}"
        event_ticker = f"{series}-{date_code}"
        actual_high = wx["temp_max_f"]

        # Generate bracket markets around typical temps
        baseline_high = {"NYC": 52, "CHI": 48, "LAX": 68, "MIA": 82}[city]
        strikes = list(range(int(baseline_high - 15), int(baseline_high + 20), 5))

        for strike in strikes:
            market_ticker = f"{event_ticker}-T{strike}"

            # True probability (with noise to simulate market inefficiency)
            true_prob = 1.0 / (1.0 + np.exp(-0.35 * (actual_high - strike)))
            # Add market noise/bias
            market_noise = np.random.normal(0, 0.06)
            # Systematic slight overconfidence bias
            bias = 0.02 if true_prob > 0.5 else -0.02
            market_prob = np.clip(true_prob + market_noise + bias, 0.02, 0.98)

            is_resolved = dt.date() < datetime.now(timezone.utc).date()
            actual_outcome = "yes" if actual_high >= strike else "no"

            for snap_idx in range(n_snapshots):
                # Prices converge toward truth over snapshots
                convergence = snap_idx / max(n_snapshots - 1, 1)
                snap_prob = market_prob * (1 - convergence * 0.3) + true_prob * (convergence * 0.3)
                snap_prob = np.clip(snap_prob, 0.02, 0.98)

                snap_time = dt - timedelta(hours=48 - snap_idx * 20) + timedelta(minutes=random.randint(0, 60))

                rows.append({
                    "snapshot_time": snap_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "source": "kalshi",
                    "market_id": market_ticker,
                    "event_id": event_ticker,
                    "series": series,
                    "title": f"{strike} degrees or above",
                    "yes_price": round(float(snap_prob), 4),
                    "no_price": round(float(1 - snap_prob), 4),
                    "yes_bid": round(float(snap_prob - 0.01), 4),
                    "yes_ask": round(float(snap_prob + 0.01), 4),
                    "volume": random.randint(100, 50000),
                    "open_interest": random.randint(50, 20000),
                    "status": "settled" if is_resolved else "active",
                    "result": actual_outcome if is_resolved else "",
                    "expiration": dt.strftime("%Y-%m-%dT23:59:00Z"),
                    "floor_strike": strike,
                    "cap_strike": "",
                })

    # Add some Polymarket-style markets
    poly_questions = [
        ("Will NYC see a temperature above 60°F this week?", 0.35),
        ("Will Miami hit 90°F before April?", 0.15),
        ("Will Chicago have snow this week?", 0.25),
        ("Will LA have rain this week?", 0.10),
        ("Will any US city break a heat record this month?", 0.05),
    ]
    for question, base_prob in poly_questions:
        prob = np.clip(base_prob + np.random.normal(0, 0.05), 0.02, 0.98)
        for snap_idx in range(n_snapshots):
            snap_time = datetime.now(timezone.utc) - timedelta(hours=48 - snap_idx * 20)
            rows.append({
                "snapshot_time": snap_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source": "polymarket",
                "market_id": f"poly-weather-{hash(question) % 10000:04d}",
                "event_id": "",
                "series": "",
                "title": question,
                "yes_price": round(float(prob + np.random.normal(0, 0.02)), 4),
                "no_price": round(float(1 - prob + np.random.normal(0, 0.02)), 4),
                "yes_bid": 0.0,
                "yes_ask": 0.0,
                "volume": random.randint(1000, 100000),
                "open_interest": 0,
                "status": "active",
                "result": "",
                "expiration": (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%dT23:59:00Z"),
                "floor_strike": "",
                "cap_strike": "",
            })

    return pd.DataFrame(rows)


def main():
    os.makedirs(config.DATA_DIR, exist_ok=True)

    # Generate 30 days of weather data
    start_date = datetime(2026, 2, 20)
    weather_df = generate_weather_actuals(start_date, days=30)
    weather_df.to_csv(config.WEATHER_ACTUALS_FILE, index=False)
    print(f"Generated {len(weather_df)} weather rows -> {config.WEATHER_ACTUALS_FILE}")

    # Generate market snapshots
    snapshots_df = generate_market_snapshots(weather_df, n_snapshots=3)
    snapshots_df.to_csv(config.MARKET_SNAPSHOTS_FILE, index=False)
    print(f"Generated {len(snapshots_df)} market snapshot rows -> {config.MARKET_SNAPSHOTS_FILE}")

    print("Seed data generation complete.")


if __name__ == "__main__":
    main()
