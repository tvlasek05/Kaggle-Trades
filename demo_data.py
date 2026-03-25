"""Generate realistic demo data for testing the analysis pipeline."""

import os
import random
from datetime import datetime, timedelta, timezone

import pandas as pd

import config

random.seed(42)

CITIES = ["new york", "chicago", "miami", "phoenix", "seattle", "denver", "houston", "boston"]
SOURCES = ["polymarket", "kalshi"]


def _random_weather_question(city, date_str, market_type="high_temp"):
    templates = {
        "high_temp": [
            f"Will the high temperature in {city.title()} exceed {{thresh}}°F on {date_str}?",
            f"Will {city.title()} reach {{thresh}}°F or above on {date_str}?",
        ],
        "low_temp": [
            f"Will the low temperature in {city.title()} drop below {{thresh}}°F on {date_str}?",
        ],
        "precip": [
            f"Will {city.title()} receive rain on {date_str}?",
            f"Will there be measurable precipitation in {city.title()} on {date_str}?",
        ],
        "snow": [
            f"Will {city.title()} get snow on {date_str}?",
        ],
        "hurricane": [
            f"Will a hurricane make landfall near {city.title()} in {{month}}?",
        ],
    }
    choices = templates.get(market_type, templates["high_temp"])
    q = random.choice(choices)
    if "{thresh}" in q:
        if market_type == "high_temp":
            q = q.format(thresh=random.choice([80, 85, 90, 95, 100]))
        elif market_type == "low_temp":
            q = q.format(thresh=random.choice([20, 25, 30, 32, 35, 40]))
    if "{month}" in q:
        q = q.format(month=random.choice(["August", "September", "October"]))
    return q


def generate_demo_markets(n_active=30, n_resolved=20):
    """Generate a mix of active and resolved weather prediction markets."""
    markets = []
    now = datetime.now(timezone.utc)

    market_types = ["high_temp", "low_temp", "precip", "snow"]

    # Resolved markets (past dates)
    for i in range(n_resolved):
        city = random.choice(CITIES)
        source = random.choice(SOURCES)
        days_ago = random.randint(7, 90)
        target_date = (now - timedelta(days=days_ago)).strftime("%B %d, %Y")
        target_date_iso = (now - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        mtype = random.choice(market_types)
        question = _random_weather_question(city, target_date, mtype)

        # Generate realistic implied probability
        implied_prob = round(random.uniform(0.1, 0.9), 2)

        # Generate outcome correlated with implied prob but with noise
        actual = 1 if random.random() < (implied_prob + random.uniform(-0.15, 0.15)) else 0
        actual = max(0, min(1, actual))

        markets.append({
            "source": source,
            "market_id": f"{source[:4].upper()}-WEATHER-{i:04d}",
            "question": question,
            "description": f"Weather market for {city.title()} on {target_date}",
            "end_date": target_date_iso,
            "resolved": True,
            "outcome": "Yes" if actual == 1 else "No",
            "volume": random.randint(500, 500000),
            "last_price": implied_prob,
            "implied_prob": implied_prob,
            "fetched_at": now.isoformat(),
            "tags": "weather",
        })

    # Active markets (future dates)
    for i in range(n_active):
        city = random.choice(CITIES)
        source = random.choice(SOURCES)
        days_ahead = random.randint(1, 30)
        target_date = (now + timedelta(days=days_ahead)).strftime("%B %d, %Y")
        target_date_iso = (now + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        mtype = random.choice(market_types)
        question = _random_weather_question(city, target_date, mtype)

        implied_prob = round(random.uniform(0.05, 0.95), 2)
        volume = random.randint(100, 300000)

        markets.append({
            "source": source,
            "market_id": f"{source[:4].upper()}-WEATHER-A{i:04d}",
            "question": question,
            "description": f"Weather market for {city.title()} on {target_date}",
            "end_date": target_date_iso,
            "resolved": False,
            "outcome": None,
            "volume": volume,
            "last_price": implied_prob,
            "implied_prob": implied_prob,
            "fetched_at": now.isoformat(),
            "tags": "weather",
        })

    return pd.DataFrame(markets)


def generate_demo_price_history(markets_df, snapshots_per_market=8):
    """Generate rolling price history with realistic drift patterns."""
    rows = []
    now = datetime.now(timezone.utc)

    for _, market in markets_df.iterrows():
        base_price = market["last_price"]
        n_snaps = random.randint(3, snapshots_per_market)

        # Some markets drift consistently (slow reaction pattern)
        drift = random.choice([0, 0, 0, 0.02, -0.02, 0.03, -0.03])

        for j in range(n_snaps):
            hours_ago = (n_snaps - j) * random.randint(4, 24)
            ts = (now - timedelta(hours=hours_ago)).isoformat()

            noise = random.gauss(0, 0.03)
            price = base_price + drift * j + noise
            price = max(0.01, min(0.99, round(price, 3)))

            rows.append({
                "source": market["source"],
                "market_id": market["market_id"],
                "timestamp": ts,
                "price": price,
                "implied_prob": price,
                "volume": market["volume"] + random.randint(-1000, 1000),
            })

    return pd.DataFrame(rows)


def generate_demo_outcomes(markets_df):
    """Generate outcome verification data for resolved markets."""
    resolved = markets_df[markets_df["resolved"] == True].copy()
    rows = []

    for _, market in resolved.iterrows():
        text = f"{market['question']} {market['description']}"
        # Extract city from description
        city = None
        for c in CITIES:
            if c in text.lower():
                city = c
                break

        if city is None:
            continue

        # Generate realistic weather data
        actual_high = random.randint(50, 105)
        actual_low = actual_high - random.randint(10, 25)

        rows.append({
            "source": market["source"],
            "market_id": market["market_id"],
            "question": market["question"],
            "city": city,
            "target_date": market["end_date"],
            "threshold_f": random.choice([80, 85, 90, 95, 32, 35, 40]),
            "actual_high_f": actual_high,
            "actual_low_f": actual_low,
            "actual_precip_in": round(random.uniform(0, 1.5), 2),
            "market_outcome": market["outcome"],
            "implied_prob": market["implied_prob"],
            "verified_at": datetime.now(timezone.utc).isoformat(),
        })

    return pd.DataFrame(rows)


def seed_demo_data():
    """Create all demo data files."""
    os.makedirs(config.DATA_DIR, exist_ok=True)
    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    print("Generating demo markets...")
    markets_df = generate_demo_markets(n_active=30, n_resolved=25)
    markets_df.to_csv(config.MARKETS_FILE, index=False)
    print(f"  Saved {len(markets_df)} markets")

    print("Generating price history...")
    prices_df = generate_demo_price_history(markets_df)
    prices_df.to_csv(config.PRICES_FILE, index=False)
    print(f"  Saved {len(prices_df)} price snapshots")

    print("Generating outcome verifications...")
    outcomes_df = generate_demo_outcomes(markets_df)
    outcomes_df.to_csv(config.OUTCOMES_FILE, index=False)
    print(f"  Saved {len(outcomes_df)} outcomes")

    print("Demo data generated successfully!")
    return markets_df, prices_df, outcomes_df


if __name__ == "__main__":
    seed_demo_data()
