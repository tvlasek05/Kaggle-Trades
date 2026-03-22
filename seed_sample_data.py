#!/usr/bin/env python3
"""Generate realistic sample data for development/demo when APIs are unavailable."""

import csv
import os
import random
from datetime import datetime, timedelta, timezone

import config

random.seed(42)


def generate_weather_actuals():
    """Generate 30 days of realistic weather data for each city."""
    # Typical March temps (F) and precip patterns
    city_profiles = {
        "NY":  {"temp_mean": 48, "temp_std": 8, "precip_rate": 0.35},
        "CHI": {"temp_mean": 42, "temp_std": 10, "precip_rate": 0.30},
        "LA":  {"temp_mean": 68, "temp_std": 5, "precip_rate": 0.10},
        "MI":  {"temp_mean": 80, "temp_std": 4, "precip_rate": 0.20},
    }

    rows = []
    base_date = datetime(2026, 2, 20)
    for city_key, profile in city_profiles.items():
        for day_offset in range(30):
            date = base_date + timedelta(days=day_offset)
            temp_max = round(random.gauss(profile["temp_mean"], profile["temp_std"]), 1)
            temp_min = round(temp_max - random.uniform(8, 18), 1)
            has_precip = random.random() < profile["precip_rate"]
            precip = round(random.uniform(0.01, 0.8), 2) if has_precip else 0.0
            snow = round(precip * random.uniform(5, 12), 1) if has_precip and temp_max < 35 else 0.0
            wind = round(random.uniform(5, 25), 1)

            rows.append({
                "city": city_key,
                "date": date.strftime("%Y-%m-%d"),
                "temp_max_f": temp_max,
                "temp_min_f": temp_min,
                "precipitation_in": precip,
                "snowfall_in": snow,
                "wind_max_mph": wind,
                "data_type": "actual",
                "fetch_time": datetime.now(timezone.utc).isoformat(),
            })

    # Also add 7-day forecast
    forecast_start = base_date + timedelta(days=30)
    for city_key, profile in city_profiles.items():
        for day_offset in range(7):
            date = forecast_start + timedelta(days=day_offset)
            temp_max = round(random.gauss(profile["temp_mean"], profile["temp_std"]), 1)
            temp_min = round(temp_max - random.uniform(8, 18), 1)
            has_precip = random.random() < profile["precip_rate"]
            precip = round(random.uniform(0.01, 0.8), 2) if has_precip else 0.0
            snow = 0.0
            wind = round(random.uniform(5, 25), 1)

            rows.append({
                "city": city_key,
                "date": date.strftime("%Y-%m-%d"),
                "temp_max_f": temp_max,
                "temp_min_f": temp_min,
                "precipitation_in": precip,
                "snowfall_in": snow,
                "wind_max_mph": wind,
                "data_type": "forecast",
                "fetch_time": datetime.now(timezone.utc).isoformat(),
            })

    return rows


def generate_market_snapshots():
    """Generate realistic Kalshi-style weather market snapshots."""
    rows = []
    base_time = datetime(2026, 2, 20, tzinfo=timezone.utc)

    # Generate markets for different cities and strike prices
    market_configs = [
        ("KXHIGHNY", "NY", [35, 40, 45, 50, 55, 60]),
        ("KXHIGHCHI", "CHI", [30, 35, 40, 45, 50, 55]),
        ("KXHIGHLA", "LA", [60, 65, 70, 75, 80]),
        ("KXHIGHMI", "MI", [72, 76, 80, 84, 88]),
    ]

    # Typical March temps for base rate estimation
    city_temps = {
        "NY": 48, "CHI": 42, "LA": 68, "MI": 80,
    }

    market_id_counter = 0
    for series, city, strikes in market_configs:
        mean_temp = city_temps[city]
        for strike in strikes:
            market_id_counter += 1
            ticker = f"{series}-26MAR22-T{strike}"

            # True probability based on normal distribution
            from scipy import stats  # noqa: delayed import
            true_prob = 1 - stats.norm.cdf(strike, loc=mean_temp, scale=8)

            # Multiple snapshots over 30 days (simulating daily pulls)
            for day_offset in range(0, 30, 3):
                snap_time = base_time + timedelta(days=day_offset, hours=random.randint(8, 20))

                # Market price drifts around true prob with noise
                noise = random.gauss(0, 0.06)
                # Add systematic bias: markets slightly overestimate extreme events
                bias = 0.03 if true_prob < 0.3 else -0.02 if true_prob > 0.7 else 0
                market_prob = max(0.01, min(0.99, true_prob + noise + bias))
                yes_price = round(market_prob * 100, 1)

                # Determine if resolved (only last snapshot for markets closing today)
                is_last = (day_offset >= 27)
                resolved = is_last and random.random() < 0.7
                status = "settled" if resolved else "open"
                result = None
                outcome = None
                if resolved:
                    # Simulate actual outcome
                    actual_temp = mean_temp + random.gauss(0, 8)
                    outcome = 1 if actual_temp >= strike else 0
                    result = "yes" if outcome == 1 else "no"

                rows.append({
                    "source": "kalshi",
                    "market_id": ticker,
                    "series_ticker": series,
                    "question": f"Will {config.CITY_COORDS[city]['name']} high temp be {strike}°F or above?",
                    "yes_price": yes_price,
                    "implied_prob": round(market_prob, 4),
                    "volume": random.randint(50, 5000),
                    "open_interest": random.randint(10, 1000),
                    "status": status,
                    "close_time": (base_time + timedelta(days=30)).isoformat(),
                    "floor_strike": strike,
                    "cap_strike": strike,
                    "result": result,
                    "outcome": outcome,
                    "snapshot_time": snap_time.isoformat(),
                })

    # Add some Polymarket weather markets
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
            outcome = None
            result = None
            if resolved:
                outcome = 1 if random.random() < true_prob else 0
                result = "Yes" if outcome == 1 else "No"

            rows.append({
                "source": "polymarket",
                "market_id": f"poly-{hash(question) % 100000:05d}",
                "series_ticker": "",
                "question": question,
                "yes_price": round(market_prob, 4),
                "implied_prob": round(market_prob, 4),
                "volume": random.randint(1000, 50000),
                "open_interest": random.randint(500, 20000),
                "status": "resolved" if resolved else "active",
                "close_time": (base_time + timedelta(days=30)).isoformat(),
                "floor_strike": None,
                "cap_strike": None,
                "result": result,
                "outcome": outcome,
                "snapshot_time": snap_time.isoformat(),
            })

    return rows


def main():
    print("Generating sample data...")

    # Weather
    weather_rows = generate_weather_actuals()
    fields = ["city", "date", "temp_max_f", "temp_min_f",
              "precipitation_in", "snowfall_in", "wind_max_mph",
              "data_type", "fetch_time"]
    with open(config.WEATHER_ACTUALS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(weather_rows)
    print(f"  Weather: {len(weather_rows)} rows -> {config.WEATHER_ACTUALS_CSV}")

    # Market snapshots
    market_rows = generate_market_snapshots()
    snap_fields = [
        "source", "market_id", "series_ticker", "question", "yes_price",
        "implied_prob", "volume", "open_interest", "status", "close_time",
        "floor_strike", "cap_strike", "result", "outcome", "snapshot_time",
    ]
    with open(config.MARKET_SNAPSHOTS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=snap_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(market_rows)
    print(f"  Snapshots: {len(market_rows)} rows -> {config.MARKET_SNAPSHOTS_CSV}")

    # Resolved subset
    resolved = [r for r in market_rows if r.get("outcome") is not None]
    with open(config.RESOLVED_MARKETS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=snap_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(resolved)
    print(f"  Resolved: {len(resolved)} rows -> {config.RESOLVED_MARKETS_CSV}")

    print("Done!")


if __name__ == "__main__":
    main()
