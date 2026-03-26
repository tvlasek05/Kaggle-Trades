#!/usr/bin/env python3
"""Generate realistic seed data for weather prediction market analysis.

Creates synthetic but realistic market data for testing and bootstrapping
the pipeline when APIs are unreachable.
"""

import csv
import random
from datetime import datetime, timedelta, timezone

import config

random.seed(42)

# Realistic temperature ranges by city and month (Fahrenheit highs)
TEMP_PROFILES = {
    "NY":  {1: (30,42), 2: (32,45), 3: (40,55), 4: (50,65), 5: (60,75), 6: (70,85),
            7: (78,92), 8: (75,90), 9: (65,80), 10: (55,68), 11: (42,55), 12: (32,45)},
    "CHI": {1: (22,35), 2: (25,38), 3: (35,50), 4: (48,62), 5: (58,72), 6: (68,83),
            7: (73,88), 8: (72,86), 9: (62,78), 10: (50,65), 11: (38,50), 12: (25,38)},
    "LA":  {1: (57,68), 2: (57,69), 3: (58,70), 4: (60,72), 5: (62,74), 6: (65,78),
            7: (70,84), 8: (71,85), 9: (70,83), 10: (66,78), 11: (60,72), 12: (56,67)},
    "MIA": {1: (68,78), 2: (69,79), 3: (72,82), 4: (75,85), 5: (78,88), 6: (80,90),
            7: (82,92), 8: (82,92), 9: (80,90), 10: (78,87), 11: (74,82), 12: (70,79)},
    "DAL": {1: (38,55), 2: (42,60), 3: (50,68), 4: (58,75), 5: (68,85), 6: (76,95),
            7: (80,100), 8: (80,100), 9: (72,92), 10: (60,78), 11: (48,65), 12: (40,56)},
    "DEN": {1: (20,45), 2: (22,47), 3: (28,54), 4: (35,60), 5: (45,70), 6: (55,82),
            7: (62,90), 8: (60,88), 9: (50,80), 10: (38,65), 11: (28,52), 12: (20,44)},
    "BOS": {1: (25,38), 2: (27,40), 3: (33,48), 4: (42,58), 5: (52,68), 6: (62,78),
            7: (68,84), 8: (66,82), 9: (58,74), 10: (48,62), 11: (38,52), 12: (28,40)},
}

SNOW_PROFILES = {
    "NY":  {1: 0.4, 2: 0.35, 3: 0.2, 11: 0.1, 12: 0.25},
    "CHI": {1: 0.45, 2: 0.4, 3: 0.25, 11: 0.15, 12: 0.35},
    "BOS": {1: 0.5, 2: 0.45, 3: 0.3, 11: 0.1, 12: 0.3},
}


def generate_actual_temp(city, month):
    lo, hi = TEMP_PROFILES[city][month]
    return round(random.gauss((lo + hi) / 2, (hi - lo) / 4), 1)


def implied_prob_for_strike(actual, strike, noise=0.08):
    """Generate a realistic market-implied probability given actual outcome."""
    if actual >= strike:
        base = random.uniform(0.55, 0.95)
    else:
        diff = strike - actual
        base = max(0.02, min(0.45, 0.5 - diff * 0.03))
    # Add market noise / mispricing
    prob = base + random.gauss(0, noise)
    return max(0.02, min(0.98, prob))


def run():
    markets = {}
    prices = []
    outcomes = {}
    now = datetime.now(timezone.utc)

    # Generate 6 months of historical data (Oct 2025 - Mar 2026)
    start = datetime(2025, 10, 1, tzinfo=timezone.utc)
    end = now - timedelta(days=2)

    temp_series = ["KXHIGHNY", "KXHIGHCHI", "KXHIGHLA", "KXHIGHMIA", "KXHIGHDAL", "KXHIGHDEN"]
    snow_series = ["KXSNOWNYC", "KXSNOWCHI", "KXSNOWBOS"]

    market_id = 0

    # Temperature markets
    for series in temp_series:
        city = config.SERIES_CITY_MAP[series]
        current = start
        while current < end:
            month = current.month
            target_date = current.strftime("%Y-%m-%d")
            actual_temp = generate_actual_temp(city, month)
            lo, hi = TEMP_PROFILES[city][month]
            mid = (lo + hi) / 2

            # Create 3-5 strike levels around the expected range
            strikes = sorted(set([
                round(mid - 10),
                round(mid - 5),
                round(mid),
                round(mid + 5),
                round(mid + 10),
            ]))

            event_ticker = f"HIGH{city}-{current.strftime('%d%b%y').upper()}"

            for strike in strikes:
                market_id += 1
                ticker = f"HIGH{city}-{current.strftime('%d%b%y').upper()}-T{int(strike)}"
                prob = implied_prob_for_strike(actual_temp, strike)
                last_price = round(prob * 100)
                yes_bid = max(1, last_price - random.randint(1, 3))
                yes_ask = min(99, last_price + random.randint(1, 3))
                resolved = current < (now - timedelta(days=5))
                actual_yes = 1 if actual_temp >= strike else 0
                volume = random.randint(50, 5000)

                record = {
                    "ticker": ticker,
                    "event_ticker": event_ticker,
                    "series_ticker": series,
                    "title": f"High temp in {city} >= {int(strike)}F on {target_date}",
                    "strike_value": strike,
                    "target_date": target_date,
                    "city": city,
                    "yes_bid": yes_bid,
                    "yes_ask": yes_ask,
                    "last_price": last_price,
                    "volume": volume,
                    "open_interest": random.randint(10, 1000),
                    "status": "settled" if resolved else "open",
                    "result": ("yes" if actual_yes else "no") if resolved else "",
                    "close_time": (current + timedelta(hours=23)).isoformat(),
                    "first_seen": (current - timedelta(days=7)).isoformat(),
                    "last_updated": now.isoformat(),
                }
                markets[ticker] = record

                # Price snapshots (3-5 per market for resolved, 1-2 for open)
                n_snaps = random.randint(3, 6) if resolved else random.randint(1, 2)
                for i in range(n_snaps):
                    snap_time = current - timedelta(days=7 - i * 2, hours=random.randint(0, 12))
                    # Price drifts toward actual outcome over time
                    # Some markets have strong late corrections (slow reactions)
                    drift_strength = 0.7 if random.random() < 0.15 else 0.3
                    drift = (actual_yes * 100 - last_price) * (i / max(n_snaps, 1)) * drift_strength
                    snap_price = max(1, min(99, round(last_price + drift + random.gauss(0, 3))))
                    prices.append({
                        "timestamp": snap_time.isoformat(),
                        "ticker": ticker,
                        "yes_bid": max(1, snap_price - random.randint(1, 3)),
                        "yes_ask": min(99, snap_price + random.randint(1, 3)),
                        "last_price": snap_price,
                        "volume": random.randint(10, volume),
                        "open_interest": random.randint(5, 500),
                    })

                # Outcomes for resolved
                if resolved:
                    key = f"{event_ticker}_{target_date}"
                    if key not in outcomes:
                        outcomes[key] = {
                            "event_ticker": event_ticker,
                            "series_ticker": series,
                            "target_date": target_date,
                            "city": city,
                            "weather_variable": "temperature_2m_max",
                            "actual_value": actual_temp,
                            "fetched_at": now.isoformat(),
                        }

            # Move to next market date (every 1-3 days)
            current += timedelta(days=random.choice([1, 2, 3]))

    # Snow markets (Nov-Mar only)
    for series in snow_series:
        city = config.SERIES_CITY_MAP[series]
        current = datetime(2025, 11, 1, tzinfo=timezone.utc)
        while current < end and current.month in (11, 12, 1, 2, 3):
            month = current.month
            target_date = current.strftime("%Y-%m-%d")
            snow_chance = SNOW_PROFILES[city].get(month, 0)
            has_snow = random.random() < snow_chance
            actual_snow = round(random.uniform(0.5, 8.0), 1) if has_snow else 0.0

            strikes = [0.1, 1.0, 2.0, 4.0]
            event_ticker = f"SNOW{city}-{current.strftime('%d%b%y').upper()}"

            for strike in strikes:
                market_id += 1
                ticker = f"SNOW{city}-{current.strftime('%d%b%y').upper()}-B{strike}"
                actual_yes = 1 if actual_snow >= strike else 0
                prob = implied_prob_for_strike(actual_snow, strike, noise=0.12)
                last_price = round(prob * 100)
                yes_bid = max(1, last_price - random.randint(1, 4))
                yes_ask = min(99, last_price + random.randint(1, 4))
                resolved = current < (now - timedelta(days=5))
                volume = random.randint(20, 2000)

                record = {
                    "ticker": ticker,
                    "event_ticker": event_ticker,
                    "series_ticker": series,
                    "title": f"Snowfall in {city} >= {strike}in on {target_date}",
                    "strike_value": strike,
                    "target_date": target_date,
                    "city": city,
                    "yes_bid": yes_bid,
                    "yes_ask": yes_ask,
                    "last_price": last_price,
                    "volume": volume,
                    "open_interest": random.randint(5, 500),
                    "status": "settled" if resolved else "open",
                    "result": ("yes" if actual_yes else "no") if resolved else "",
                    "close_time": (current + timedelta(hours=23)).isoformat(),
                    "first_seen": (current - timedelta(days=5)).isoformat(),
                    "last_updated": now.isoformat(),
                }
                markets[ticker] = record

                n_snaps = random.randint(2, 5) if resolved else 1
                for i in range(n_snaps):
                    snap_time = current - timedelta(days=5 - i, hours=random.randint(0, 12))
                    drift = (actual_yes * 100 - last_price) * (i / max(n_snaps, 1)) * 0.25
                    snap_price = max(1, min(99, round(last_price + drift + random.gauss(0, 4))))
                    prices.append({
                        "timestamp": snap_time.isoformat(),
                        "ticker": ticker,
                        "yes_bid": max(1, snap_price - random.randint(1, 4)),
                        "yes_ask": min(99, snap_price + random.randint(1, 4)),
                        "last_price": snap_price,
                        "volume": random.randint(5, volume),
                        "open_interest": random.randint(2, 200),
                    })

                if resolved:
                    key = f"{event_ticker}_{target_date}"
                    if key not in outcomes:
                        outcomes[key] = {
                            "event_ticker": event_ticker,
                            "series_ticker": series,
                            "target_date": target_date,
                            "city": city,
                            "weather_variable": "snowfall_sum",
                            "actual_value": actual_snow,
                            "fetched_at": now.isoformat(),
                        }

            current += timedelta(days=random.choice([3, 5, 7]))

    # Write markets CSV
    fieldnames_m = [
        "ticker", "event_ticker", "series_ticker", "title",
        "strike_value", "target_date", "city",
        "yes_bid", "yes_ask", "last_price", "volume", "open_interest",
        "status", "result", "close_time", "first_seen", "last_updated",
    ]
    with open(config.MARKETS_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames_m, extrasaction="ignore")
        w.writeheader()
        for row in sorted(markets.values(), key=lambda r: r["ticker"]):
            w.writerow(row)

    # Write prices CSV
    fieldnames_p = ["timestamp", "ticker", "yes_bid", "yes_ask", "last_price", "volume", "open_interest"]
    with open(config.PRICES_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames_p, extrasaction="ignore")
        w.writeheader()
        for row in sorted(prices, key=lambda r: r["timestamp"]):
            w.writerow(row)

    # Write outcomes CSV
    fieldnames_o = ["event_ticker", "series_ticker", "target_date", "city", "weather_variable", "actual_value", "fetched_at"]
    with open(config.OUTCOMES_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames_o, extrasaction="ignore")
        w.writeheader()
        for row in sorted(outcomes.values(), key=lambda r: r["target_date"]):
            w.writerow(row)

    print(f"Seed data generated:")
    print(f"  Markets: {len(markets)}")
    print(f"  Price snapshots: {len(prices)}")
    print(f"  Weather outcomes: {len(outcomes)}")


if __name__ == "__main__":
    run()
