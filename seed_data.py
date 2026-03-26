#!/usr/bin/env python3
"""Generate realistic seed data for the weather prediction market pipeline.

Used when live APIs (Kalshi, Open-Meteo, Polymarket) are unavailable.
Produces markets, price snapshots, and weather outcomes that exercise
all analysis paths: calibration, mispricing, bias detection, slow reactions.
"""

import csv
import random
from datetime import datetime, timedelta, timezone

import config

random.seed(42)

# Realistic seasonal temperature ranges (°F) by city and month
TEMP_PROFILES = {
    "NY":  {1: (28, 40), 2: (30, 43), 3: (37, 52), 4: (47, 63), 5: (56, 73), 6: (66, 82),
            7: (71, 87), 8: (70, 85), 9: (62, 78), 10: (51, 66), 11: (41, 55), 12: (31, 43)},
    "CHI": {1: (18, 32), 2: (21, 36), 3: (31, 48), 4: (41, 60), 5: (51, 71), 6: (62, 81),
            7: (67, 85), 8: (66, 83), 9: (57, 76), 10: (45, 63), 11: (33, 48), 12: (21, 34)},
    "LA":  {1: (49, 68), 2: (50, 69), 3: (52, 70), 4: (54, 72), 5: (57, 74), 6: (61, 78),
            7: (64, 84), 8: (65, 85), 9: (63, 83), 10: (59, 79), 11: (53, 73), 12: (48, 67)},
    "MIA": {1: (62, 77), 2: (63, 78), 3: (66, 80), 4: (69, 83), 5: (73, 87), 6: (76, 89),
            7: (77, 91), 8: (77, 91), 9: (76, 89), 10: (73, 86), 11: (68, 82), 12: (64, 78)},
    "DAL": {1: (36, 56), 2: (39, 60), 3: (47, 68), 4: (55, 77), 5: (64, 85), 6: (72, 93),
            7: (76, 97), 8: (76, 97), 9: (68, 90), 10: (57, 79), 11: (46, 67), 12: (37, 57)},
    "DEN": {1: (16, 45), 2: (18, 47), 3: (25, 54), 4: (33, 60), 5: (43, 70), 6: (52, 82),
            7: (58, 90), 8: (57, 87), 9: (47, 79), 10: (35, 65), 11: (24, 52), 12: (16, 44)},
    "BOS": {1: (22, 36), 2: (24, 39), 3: (31, 47), 4: (41, 57), 5: (50, 67), 6: (60, 77),
            7: (66, 82), 8: (65, 80), 9: (57, 73), 10: (46, 62), 11: (37, 52), 12: (27, 40)},
}

# Snowfall probability by month (inches if it snows)
SNOW_PROFILES = {
    "NY":  {1: (0.4, 4.0), 2: (0.4, 3.5), 3: (0.2, 2.0), 11: (0.1, 1.0), 12: (0.3, 3.0)},
    "CHI": {1: (0.5, 5.0), 2: (0.4, 4.0), 3: (0.3, 3.0), 11: (0.2, 2.0), 12: (0.4, 4.5)},
    "BOS": {1: (0.5, 5.5), 2: (0.5, 5.0), 3: (0.3, 3.5), 11: (0.1, 1.5), 12: (0.4, 4.0)},
}

MONTHS_3LETTER = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
    7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC",
}

TEMP_SERIES = {
    "NY": "KXHIGHNY", "CHI": "KXHIGHCHI", "LA": "KXHIGHLA",
    "MIA": "KXHIGHMIA", "DAL": "KXHIGHDAL", "DEN": "KXHIGHDEN",
}
SNOW_SERIES = {"NY": "KXSNOWNYC", "CHI": "KXSNOWCHI", "BOS": "KXSNOWBOS"}


def generate_actual_temp(city, month):
    """Generate a realistic actual temperature for city/month."""
    lo, hi = TEMP_PROFILES[city][month]
    mean = (lo + hi) / 2
    std = (hi - lo) / 4
    return round(random.gauss(mean, std), 1)


def generate_actual_snow(city, month):
    """Generate actual snowfall (inches). Returns 0 in non-snow months."""
    profile = SNOW_PROFILES.get(city, {})
    if month not in profile:
        return 0.0
    prob, max_inches = profile[month]
    if random.random() > prob:
        return 0.0
    return round(random.uniform(0.1, max_inches), 1)


def implied_price_from_outcome(actual, strike, is_high=True, noise=0.0):
    """Generate a market price that's correlated with the actual outcome but with noise."""
    if is_high:
        resolved_yes = actual >= strike
    else:
        resolved_yes = actual >= strike

    # Base price near the true outcome
    if resolved_yes:
        base = random.uniform(55, 95)
    else:
        base = random.uniform(5, 45)

    # Add systematic noise (mispricing)
    price = base + noise
    return max(1, min(99, round(price)))


def generate_markets():
    """Generate full set of market data, outcomes, and price snapshots."""
    markets = {}
    outcomes = {}
    price_snapshots = []

    now = datetime.now(timezone.utc)
    base_date = now - timedelta(days=90)  # Start 90 days ago

    market_id = 0

    # --- Temperature markets: 6 cities, ~60 days each ---
    for city, series in TEMP_SERIES.items():
        for day_offset in range(75):
            target_dt = base_date + timedelta(days=day_offset)
            target_date = target_dt.strftime("%Y-%m-%d")
            month = target_dt.month
            day = target_dt.day

            actual_temp = generate_actual_temp(city, month)
            lo, hi = TEMP_PROFILES[city][month]
            mean_temp = (lo + hi) / 2

            # Generate 3-5 strike levels around the mean
            strikes = sorted(set([
                round(mean_temp - 10),
                round(mean_temp - 5),
                round(mean_temp),
                round(mean_temp + 5),
                round(mean_temp + 10),
            ]))

            date_code = f"{day:02d}{MONTHS_3LETTER[month]}{target_dt.year % 100}"
            event_ticker = f"HIGH{city}-{date_code}"

            is_resolved = target_dt < (now - timedelta(days=5))
            is_open = target_dt >= (now - timedelta(days=1))

            for strike in strikes:
                market_id += 1
                ticker = f"{event_ticker}-T{strike}"

                # Add city-specific bias: Denver overprices YES, Miami underprices
                city_bias = {"DEN": 8, "MIA": -6, "DAL": 4, "CHI": -3}.get(city, 0)
                # Add seasonal bias: winter months have more error
                month_bias = 5 if month in (12, 1, 2) else -2 if month in (6, 7, 8) else 0

                noise = city_bias + month_bias + random.gauss(0, 8)
                last_price = implied_price_from_outcome(actual_temp, strike, noise=noise)

                if is_resolved:
                    result = "yes" if actual_temp >= strike else "no"
                    status = "settled"
                elif is_open:
                    result = ""
                    status = "open"
                else:
                    result = ""
                    status = "closed"

                yes_bid = max(1, last_price - random.randint(1, 3))
                yes_ask = min(99, last_price + random.randint(1, 3))

                first_seen = (target_dt - timedelta(days=random.randint(2, 7))).isoformat()

                record = {
                    "ticker": ticker,
                    "event_ticker": event_ticker,
                    "series_ticker": series,
                    "title": f"High temp in {city} on {target_date} >= {strike}°F",
                    "strike_value": str(strike),
                    "target_date": target_date,
                    "city": city,
                    "yes_bid": str(yes_bid),
                    "yes_ask": str(yes_ask),
                    "last_price": str(last_price),
                    "volume": str(random.randint(50, 5000)),
                    "open_interest": str(random.randint(10, 2000)),
                    "status": status,
                    "result": result,
                    "close_time": (target_dt + timedelta(hours=23)).isoformat(),
                    "first_seen": first_seen,
                    "last_updated": now.isoformat(),
                }
                markets[ticker] = record

                # Outcome for resolved markets
                if is_resolved:
                    okey = f"{event_ticker}_{target_date}"
                    if okey not in outcomes:
                        outcomes[okey] = {
                            "event_ticker": event_ticker,
                            "series_ticker": series,
                            "target_date": target_date,
                            "city": city,
                            "weather_variable": "temperature_2m_max",
                            "actual_value": str(actual_temp),
                            "fetched_at": now.isoformat(),
                        }

                # Price snapshots (2-5 per market for resolved, 1 for open)
                n_snapshots = random.randint(2, 5) if is_resolved else 1
                for snap_i in range(n_snapshots):
                    snap_time = target_dt - timedelta(
                        days=n_snapshots - snap_i,
                        hours=random.randint(0, 12)
                    )
                    # Early snapshots are noisier
                    early_noise = noise + random.gauss(0, 15) if snap_i == 0 else noise + random.gauss(0, 5)
                    snap_price = implied_price_from_outcome(actual_temp, strike, noise=early_noise)
                    price_snapshots.append({
                        "timestamp": snap_time.isoformat(),
                        "ticker": ticker,
                        "yes_bid": str(max(1, snap_price - random.randint(1, 3))),
                        "yes_ask": str(min(99, snap_price + random.randint(1, 3))),
                        "last_price": str(snap_price),
                        "volume": str(random.randint(10, 3000)),
                        "open_interest": str(random.randint(5, 1500)),
                    })

    # --- Snow markets: 3 cities, winter months only ---
    for city, series in SNOW_SERIES.items():
        for day_offset in range(75):
            target_dt = base_date + timedelta(days=day_offset)
            target_date = target_dt.strftime("%Y-%m-%d")
            month = target_dt.month

            if month not in SNOW_PROFILES.get(city, {}):
                continue

            actual_snow = generate_actual_snow(city, month)
            day = target_dt.day
            date_code = f"{day:02d}{MONTHS_3LETTER[month]}{target_dt.year % 100}"
            event_ticker = f"SNOW{city}-{date_code}"

            is_resolved = target_dt < (now - timedelta(days=5))

            for strike in [0.1, 1.0, 2.0, 4.0]:
                market_id += 1
                ticker = f"{event_ticker}-B{strike}"

                noise = random.gauss(5, 10)  # Snow markets tend to overprice
                last_price = implied_price_from_outcome(actual_snow, strike, noise=noise)

                if is_resolved:
                    result = "yes" if actual_snow >= strike else "no"
                    status = "settled"
                else:
                    result = ""
                    status = "open"

                yes_bid = max(1, last_price - random.randint(1, 4))
                yes_ask = min(99, last_price + random.randint(1, 4))

                record = {
                    "ticker": ticker,
                    "event_ticker": event_ticker,
                    "series_ticker": series,
                    "title": f"Snowfall in {city} on {target_date} >= {strike} inches",
                    "strike_value": str(strike),
                    "target_date": target_date,
                    "city": city,
                    "yes_bid": str(yes_bid),
                    "yes_ask": str(yes_ask),
                    "last_price": str(last_price),
                    "volume": str(random.randint(20, 2000)),
                    "open_interest": str(random.randint(5, 800)),
                    "status": status,
                    "result": result,
                    "close_time": (target_dt + timedelta(hours=23)).isoformat(),
                    "first_seen": (target_dt - timedelta(days=3)).isoformat(),
                    "last_updated": now.isoformat(),
                }
                markets[ticker] = record

                if is_resolved:
                    okey = f"{event_ticker}_{target_date}"
                    if okey not in outcomes:
                        outcomes[okey] = {
                            "event_ticker": event_ticker,
                            "series_ticker": series,
                            "target_date": target_date,
                            "city": city,
                            "weather_variable": "snowfall_sum",
                            "actual_value": str(actual_snow),
                            "fetched_at": now.isoformat(),
                        }

                n_snapshots = random.randint(2, 4) if is_resolved else 1
                for snap_i in range(n_snapshots):
                    snap_time = target_dt - timedelta(days=n_snapshots - snap_i)
                    early_noise = noise + random.gauss(0, 20) if snap_i == 0 else noise + random.gauss(0, 5)
                    snap_price = implied_price_from_outcome(actual_snow, strike, noise=early_noise)
                    price_snapshots.append({
                        "timestamp": snap_time.isoformat(),
                        "ticker": ticker,
                        "yes_bid": str(max(1, snap_price - 2)),
                        "yes_ask": str(min(99, snap_price + 2)),
                        "last_price": str(snap_price),
                        "volume": str(random.randint(5, 1000)),
                        "open_interest": str(random.randint(2, 500)),
                    })

    return markets, outcomes, price_snapshots


def save_all(markets, outcomes, price_snapshots):
    """Write generated data to CSV files matching the pipeline's expected format."""
    # Markets
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

    # Prices
    price_fields = ["timestamp", "ticker", "yes_bid", "yes_ask", "last_price", "volume", "open_interest"]
    with open(config.PRICES_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=price_fields, extrasaction="ignore")
        writer.writeheader()
        for row in sorted(price_snapshots, key=lambda r: r["timestamp"]):
            writer.writerow(row)

    # Outcomes
    outcome_fields = [
        "event_ticker", "series_ticker", "target_date", "city",
        "weather_variable", "actual_value", "fetched_at",
    ]
    with open(config.OUTCOMES_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=outcome_fields, extrasaction="ignore")
        writer.writeheader()
        for row in sorted(outcomes.values(), key=lambda r: r["target_date"]):
            writer.writerow(row)

    print(f"Generated {len(markets)} markets")
    print(f"Generated {len(outcomes)} weather outcomes")
    print(f"Generated {len(price_snapshots)} price snapshots")


def main():
    markets, outcomes, price_snapshots = generate_markets()
    save_all(markets, outcomes, price_snapshots)
    print(f"\nData saved to {config.DATA_DIR}/")
    print("Run 'python main.py --analyze' to analyze.")


if __name__ == "__main__":
    main()
