#!/usr/bin/env python3
"""Generate realistic seed data for testing the pipeline when APIs are unavailable.

Creates synthetic but realistic weather prediction market data based on actual
temperature patterns for tracked cities. This allows the full analysis pipeline
to be validated end-to-end.
"""

import csv
import random
from datetime import datetime, timedelta, timezone

import config

random.seed(42)

# Realistic daily high temperature ranges by city and month (Fahrenheit)
TEMP_RANGES = {
    "NY":  {1: (30, 42), 2: (32, 45), 3: (40, 55), 4: (50, 65), 5: (60, 75), 6: (70, 85),
            7: (75, 90), 8: (74, 88), 9: (65, 80), 10: (55, 68), 11: (42, 55), 12: (32, 44)},
    "CHI": {1: (25, 35), 2: (28, 40), 3: (38, 52), 4: (48, 62), 5: (60, 74), 6: (70, 84),
            7: (75, 88), 8: (73, 86), 9: (64, 78), 10: (52, 65), 11: (38, 50), 12: (26, 36)},
    "LA":  {1: (60, 68), 2: (60, 69), 3: (61, 70), 4: (63, 73), 5: (65, 74), 6: (69, 79),
            7: (73, 84), 8: (74, 85), 9: (73, 83), 10: (69, 78), 11: (64, 72), 12: (59, 67)},
    "MIA": {1: (70, 77), 2: (71, 78), 3: (73, 80), 4: (76, 83), 5: (79, 87), 6: (81, 89),
            7: (82, 91), 8: (82, 91), 9: (81, 89), 10: (79, 85), 11: (75, 81), 12: (71, 78)},
    "DAL": {1: (42, 56), 2: (46, 60), 3: (54, 68), 4: (63, 77), 5: (72, 85), 6: (80, 95),
            7: (84, 100), 8: (84, 99), 9: (77, 92), 10: (67, 80), 11: (54, 67), 12: (44, 57)},
    "DEN": {1: (32, 46), 2: (34, 48), 3: (40, 54), 4: (47, 61), 5: (56, 70), 6: (65, 82),
            7: (72, 90), 8: (70, 88), 9: (62, 80), 10: (50, 66), 11: (38, 52), 12: (30, 44)},
    "BOS": {1: (28, 38), 2: (30, 40), 3: (36, 48), 4: (46, 58), 5: (56, 68), 6: (66, 78),
            7: (72, 84), 8: (70, 82), 9: (62, 74), 10: (52, 64), 11: (40, 52), 12: (30, 40)},
}

# Snowfall probability by month (inches per day when it snows)
SNOW_PROB = {
    "NY":  {1: 0.15, 2: 0.15, 3: 0.08, 4: 0.01, 11: 0.04, 12: 0.10},
    "CHI": {1: 0.18, 2: 0.15, 3: 0.10, 4: 0.02, 11: 0.06, 12: 0.14},
    "BOS": {1: 0.20, 2: 0.18, 3: 0.12, 4: 0.02, 11: 0.05, 12: 0.15},
}


def generate_actual_temp(city: str, date: datetime) -> float:
    """Generate a realistic actual temperature for a city on a date."""
    month = date.month
    lo, hi = TEMP_RANGES.get(city, {}).get(month, (50, 70))
    mean = (lo + hi) / 2
    std = (hi - lo) / 4
    return round(random.gauss(mean, std), 1)


def generate_actual_snow(city: str, date: datetime) -> float:
    """Generate a realistic snowfall amount."""
    month = date.month
    prob = SNOW_PROB.get(city, {}).get(month, 0)
    if random.random() > prob:
        return 0.0
    return round(random.expovariate(1 / 2.5), 1)


def generate_market_price(actual: float, strike: float, series_type: str) -> dict:
    """Generate a market price that's correlated with actual outcome but with noise.

    Simulates realistic market behavior: prices roughly reflect true probability
    but with noise, occasional mispricings, and bid-ask spreads.
    """
    if series_type == "HIGH":
        true_prob = 1.0 / (1.0 + pow(2.718, -(actual - strike) / 3.0))
    else:  # SNOW
        if strike == 0:
            true_prob = 0.95 if actual > 0 else 0.05
        else:
            true_prob = 1.0 / (1.0 + pow(2.718, -(actual - strike) / 1.5))

    # Add market noise (mispricings)
    noise = random.gauss(0, 0.08)
    market_prob = max(0.02, min(0.98, true_prob + noise))

    last_price = round(market_prob * 100)
    spread = random.choice([1, 2, 3, 4, 5])
    yes_bid = max(1, last_price - spread // 2)
    yes_ask = min(99, last_price + (spread + 1) // 2)
    volume = random.randint(10, 2000)
    oi = random.randint(50, 5000)

    return {
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "last_price": last_price,
        "volume": volume,
        "open_interest": oi,
    }


def run():
    """Generate seed data covering the past 90 days."""
    now = datetime.now(timezone.utc)
    start_date = now - timedelta(days=90)

    markets = {}
    price_snapshots = []
    outcomes = {}

    temp_series = [
        ("KXHIGHNY", "NY"), ("KXHIGHCHI", "CHI"), ("KXHIGHLA", "LA"),
        ("KXHIGHMIA", "MIA"), ("KXHIGHDAL", "DAL"), ("KXHIGHDEN", "DEN"),
    ]
    snow_series = [
        ("KXSNOWNYC", "NY"), ("KXSNOWCHI", "CHI"), ("KXSNOWBOS", "BOS"),
    ]

    strikes_temp = [30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90]
    strikes_snow = [0, 1, 2, 4, 6]

    day = start_date
    while day < now:
        date_str = day.strftime("%Y-%m-%d")
        date_code = day.strftime("%d%b%y").upper()  # e.g. "26MAR26"
        is_past = day < (now - timedelta(days=5))

        # Temperature markets
        for series_ticker, city in temp_series:
            actual = generate_actual_temp(city, day)
            event_short = series_ticker.replace("KX", "")
            event_ticker = f"{event_short}-{date_code}"

            relevant_strikes = [s for s in strikes_temp
                                if abs(s - actual) <= 20]

            for strike in relevant_strikes:
                ticker = f"{event_ticker}-T{strike}"
                prices = generate_market_price(actual, strike, "HIGH")
                resolved = is_past
                result = ""
                if resolved:
                    result = "yes" if actual >= strike else "no"

                status = "settled" if resolved else "open"
                record = {
                    "ticker": ticker,
                    "event_ticker": event_ticker,
                    "series_ticker": series_ticker,
                    "title": f"Will {city} high be {strike}F or above on {date_str}?",
                    "strike_value": strike,
                    "target_date": date_str,
                    "city": city,
                    "status": status,
                    "result": result,
                    "close_time": (day + timedelta(hours=23)).isoformat(),
                    "first_seen": (day - timedelta(days=1)).isoformat(),
                    "last_updated": now.isoformat(),
                    **prices,
                }
                markets[ticker] = record

                # Multiple price snapshots for time series
                for hours_before in [48, 24, 12, 6, 2]:
                    snap_time = day - timedelta(hours=hours_before)
                    if snap_time < start_date:
                        continue
                    # Price moves toward true value as event approaches
                    time_factor = hours_before / 48.0
                    snap_noise = random.gauss(0, 0.05 * time_factor)
                    snap_price = max(1, min(99, prices["last_price"] + int(snap_noise * 100)))
                    price_snapshots.append({
                        "timestamp": snap_time.isoformat(),
                        "ticker": ticker,
                        "yes_bid": max(1, snap_price - 2),
                        "yes_ask": min(99, snap_price + 2),
                        "last_price": snap_price,
                        "volume": random.randint(5, 500),
                        "open_interest": random.randint(20, 2000),
                    })

            # Record actual weather outcome
            if is_past:
                key = f"{event_ticker}_{date_str}"
                outcomes[key] = {
                    "event_ticker": event_ticker,
                    "series_ticker": series_ticker,
                    "target_date": date_str,
                    "city": city,
                    "weather_variable": "temperature_2m_max",
                    "actual_value": actual,
                    "fetched_at": now.isoformat(),
                }

        # Snow markets (only Nov-Mar)
        if day.month in (11, 12, 1, 2, 3):
            for series_ticker, city in snow_series:
                actual_snow = generate_actual_snow(city, day)
                event_short = series_ticker.replace("KX", "")
                event_ticker = f"{event_short}-{date_code}"

                for strike in strikes_snow:
                    ticker = f"{event_ticker}-B{strike}"
                    prices = generate_market_price(actual_snow, strike, "SNOW")
                    resolved = is_past
                    result = ""
                    if resolved:
                        result = "yes" if actual_snow >= strike else "no"

                    status = "settled" if resolved else "open"
                    record = {
                        "ticker": ticker,
                        "event_ticker": event_ticker,
                        "series_ticker": series_ticker,
                        "title": f"Will {city} snowfall be {strike}in or more on {date_str}?",
                        "strike_value": strike,
                        "target_date": date_str,
                        "city": city,
                        "status": status,
                        "result": result,
                        "close_time": (day + timedelta(hours=23)).isoformat(),
                        "first_seen": (day - timedelta(days=1)).isoformat(),
                        "last_updated": now.isoformat(),
                        **prices,
                    }
                    markets[ticker] = record

                    for hours_before in [48, 24, 12, 6, 2]:
                        snap_time = day - timedelta(hours=hours_before)
                        if snap_time < start_date:
                            continue
                        time_factor = hours_before / 48.0
                        snap_noise = random.gauss(0, 0.05 * time_factor)
                        snap_price = max(1, min(99, prices["last_price"] + int(snap_noise * 100)))
                        price_snapshots.append({
                            "timestamp": snap_time.isoformat(),
                            "ticker": ticker,
                            "yes_bid": max(1, snap_price - 2),
                            "yes_ask": min(99, snap_price + 2),
                            "last_price": snap_price,
                            "volume": random.randint(1, 200),
                            "open_interest": random.randint(10, 500),
                        })

                if is_past:
                    key = f"{event_ticker}_{date_str}"
                    outcomes[key] = {
                        "event_ticker": event_ticker,
                        "series_ticker": series_ticker,
                        "target_date": date_str,
                        "city": city,
                        "weather_variable": "snowfall_sum",
                        "actual_value": actual_snow,
                        "fetched_at": now.isoformat(),
                    }

        day += timedelta(days=1)

    # Save everything
    _save_markets(markets)
    _save_prices(price_snapshots)
    _save_outcomes(outcomes)

    print(f"Seed data generated:")
    print(f"  Markets: {len(markets)}")
    print(f"  Price snapshots: {len(price_snapshots)}")
    print(f"  Weather outcomes: {len(outcomes)}")


def _save_markets(markets):
    fieldnames = [
        "ticker", "event_ticker", "series_ticker", "title",
        "strike_value", "target_date", "city",
        "yes_bid", "yes_ask", "last_price", "volume", "open_interest",
        "status", "result", "close_time",
        "first_seen", "last_updated",
    ]
    with open(config.MARKETS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in sorted(markets.values(), key=lambda r: r["ticker"]):
            writer.writerow(row)


def _save_prices(snapshots):
    fieldnames = [
        "timestamp", "ticker", "yes_bid", "yes_ask", "last_price",
        "volume", "open_interest",
    ]
    with open(config.PRICES_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in sorted(snapshots, key=lambda r: (r["timestamp"], r["ticker"])):
            writer.writerow(row)


def _save_outcomes(outcomes):
    fieldnames = [
        "event_ticker", "series_ticker", "target_date", "city",
        "weather_variable", "actual_value", "fetched_at",
    ]
    with open(config.OUTCOMES_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in sorted(outcomes.values(), key=lambda r: r["target_date"]):
            writer.writerow(row)


if __name__ == "__main__":
    run()
