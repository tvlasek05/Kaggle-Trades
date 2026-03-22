#!/usr/bin/env python3
"""Generate realistic sample data for development/demo when APIs are unavailable."""

import os
import random
from datetime import datetime, timedelta, timezone

import pandas as pd

import config

random.seed(42)


def generate_weather_actuals():
    """Generate 30 days of realistic weather data for each city."""
    city_profiles = {
        "NYC": {"temp_mean": 48, "temp_std": 8, "precip_rate": 0.35},
        "CHI": {"temp_mean": 42, "temp_std": 10, "precip_rate": 0.30},
        "LAX": {"temp_mean": 68, "temp_std": 5, "precip_rate": 0.10},
        "MIA": {"temp_mean": 80, "temp_std": 4, "precip_rate": 0.20},
    }

    rows = []
    base_date = datetime(2026, 2, 20)
    for city_key, profile in city_profiles.items():
        for day_offset in range(31):
            date = base_date + timedelta(days=day_offset)
            temp_max = round(random.gauss(profile["temp_mean"], profile["temp_std"]), 1)
            temp_min = round(temp_max - random.uniform(8, 18), 1)
            has_precip = random.random() < profile["precip_rate"]
            precip = round(random.uniform(0.2, 20), 1) if has_precip else 0.0
            snow = round(precip * random.uniform(2, 8), 1) if has_precip and temp_max < 35 else 0.0
            wind = round(random.uniform(5, 40), 1)

            rows.append({
                "date": date.strftime("%Y-%m-%d"),
                "city": city_key,
                "temp_max_f": temp_max,
                "temp_min_f": temp_min,
                "precipitation_mm": precip,
                "snowfall_mm": snow,
                "weathercode": random.choice([0, 1, 2, 3, 45, 61, 71]) if has_precip else random.choice([0, 1, 2]),
                "windspeed_max_kmh": wind,
            })

    return rows


def generate_market_snapshots():
    """Generate realistic Kalshi-style weather market snapshots."""
    from scipy import stats

    rows = []
    base_time = datetime(2026, 2, 20, tzinfo=timezone.utc)

    market_configs = [
        ("KXHIGHNY", "NYC", [35, 40, 45, 50, 55, 60]),
        ("KXHIGHCHI", "CHI", [30, 35, 40, 45, 50, 55]),
        ("KXHIGHLAX", "LAX", [60, 65, 70, 75, 80]),
        ("KXHIGHMI", "MIA", [72, 76, 80, 84, 88]),
    ]

    city_temps = {"NYC": 48, "CHI": 42, "LAX": 68, "MIA": 80}

    for series, city, strikes in market_configs:
        mean_temp = city_temps[city]
        for strike in strikes:
            # Event ticker encodes date
            event_ticker = f"{series}-22MAR26"
            true_prob = 1 - stats.norm.cdf(strike, loc=mean_temp, scale=8)

            for day_offset in range(0, 30, 3):
                snap_time = base_time + timedelta(days=day_offset, hours=random.randint(8, 20))

                noise = random.gauss(0, 0.06)
                bias = 0.03 if true_prob < 0.3 else -0.02 if true_prob > 0.7 else 0
                market_prob = max(0.01, min(0.99, true_prob + noise + bias))

                is_last = (day_offset >= 27)
                resolved = is_last and random.random() < 0.7
                status = "settled" if resolved else "active"
                result = ""
                if resolved:
                    actual_temp = mean_temp + random.gauss(0, 8)
                    result = "yes" if actual_temp >= strike else "no"

                rows.append({
                    "snapshot_time": snap_time.isoformat(),
                    "source": "kalshi",
                    "market_id": f"{series}-22MAR26-T{strike}",
                    "event_id": event_ticker,
                    "series": series,
                    "title": f"Will {city} high temp be {strike}F or above on Mar 22?",
                    "yes_price": round(market_prob, 4),
                    "no_price": round(1 - market_prob, 4),
                    "yes_bid": round(max(0, market_prob - 0.02), 4),
                    "yes_ask": round(min(1, market_prob + 0.02), 4),
                    "volume": random.randint(50, 5000),
                    "open_interest": random.randint(10, 1000),
                    "status": status,
                    "result": result,
                    "expiration": (base_time + timedelta(days=30)).isoformat(),
                    "floor_strike": strike,
                    "cap_strike": strike,
                })

    # Polymarket markets
    poly_questions = [
        ("Will NYC see a blizzard in March 2026?", 0.08),
        ("Will Miami hit 90F in March 2026?", 0.25),
        ("Will a Category 4+ hurricane form before April 2026?", 0.02),
        ("Will LA get more than 1 inch of rain in a day in March?", 0.12),
        ("Will Chicago have a day below 0F in March 2026?", 0.05),
    ]

    for question, true_prob in poly_questions:
        for day_offset in range(0, 30, 5):
            snap_time = base_time + timedelta(days=day_offset)
            noise = random.gauss(0, 0.04)
            market_prob = max(0.01, min(0.99, true_prob + noise))

            resolved = day_offset >= 25
            result = ""
            if resolved:
                result = "yes" if random.random() < true_prob else "no"

            rows.append({
                "snapshot_time": snap_time.isoformat(),
                "source": "polymarket",
                "market_id": f"poly-{abs(hash(question)) % 100000:05d}",
                "event_id": "",
                "series": "",
                "title": question,
                "yes_price": round(market_prob, 4),
                "no_price": round(1 - market_prob, 4),
                "yes_bid": 0.0,
                "yes_ask": 0.0,
                "volume": random.randint(1000, 50000),
                "open_interest": 0,
                "status": "closed" if resolved else "active",
                "result": result if resolved else "",
                "expiration": (base_time + timedelta(days=30)).isoformat(),
                "floor_strike": None,
                "cap_strike": None,
            })

    return rows


def main():
    os.makedirs(config.DATA_DIR, exist_ok=True)
    print("Generating sample data...")

    # Weather
    weather_rows = generate_weather_actuals()
    pd.DataFrame(weather_rows).to_csv(config.WEATHER_ACTUALS_FILE, index=False)
    print(f"  Weather: {len(weather_rows)} rows -> {config.WEATHER_ACTUALS_FILE}")

    # Market snapshots
    market_rows = generate_market_snapshots()
    pd.DataFrame(market_rows).to_csv(config.MARKET_SNAPSHOTS_FILE, index=False)
    print(f"  Snapshots: {len(market_rows)} rows -> {config.MARKET_SNAPSHOTS_FILE}")

    print("Done!")


if __name__ == "__main__":
    main()
