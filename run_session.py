#!/usr/bin/env python3
"""
Weather Prediction Market Tracker — Session Runner

Each run:
1. Pulls latest weather prediction market data from Polymarket & Kalshi
2. Appends new data to persistent CSV dataset
3. Maintains rolling price time series
4. Fetches actual weather outcomes for resolved markets
5. Computes calibration, forecast error, and bias metrics
6. Identifies mispriced markets and trading opportunities
7. Saves everything and outputs a concise summary
"""

import sys
import os

# Ensure we can import our modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from datetime import datetime, timezone

import config
from fetch_markets import fetch_all_weather_markets, save_markets, save_price_history
from fetch_weather import fetch_weather_for_resolved_markets
from analyze import (
    compute_calibration,
    identify_mispriced_markets,
    detect_slow_reactions,
    detect_seasonal_biases,
    generate_summary,
)


def run_session():
    """Execute a full data collection and analysis session."""
    print(f"\n{'='*60}")
    print(f"SESSION START: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*60}\n")

    # Step 1: Fetch market data from APIs
    print("STEP 1: Fetching weather prediction market data...")
    markets = fetch_all_weather_markets()

    # Step 2: Save to persistent dataset
    print("\nSTEP 2: Saving market data...")
    if markets:
        markets_df = save_markets(markets)
        prices_df = save_price_history(markets)
    else:
        print("No new API data fetched (APIs may be unreachable).")
        print("Checking for existing dataset...")
        # If no existing data, run seed to bootstrap from web-scraped data
        if not os.path.exists(config.MARKETS_FILE):
            print("No existing data found. Running initial seed...")
            from seed_data import seed
            markets_df, prices_df = seed()
        else:
            markets_df = pd.read_csv(config.MARKETS_FILE)
            try:
                prices_df = pd.read_csv(config.PRICES_FILE)
            except FileNotFoundError:
                prices_df = pd.DataFrame()
            print(f"Loaded {len(markets_df)} existing market records.")

    # Step 3: Fetch actual weather outcomes for resolved markets
    print("\nSTEP 3: Fetching weather actuals for resolved markets...")
    if not markets_df.empty:
        fetch_weather_for_resolved_markets(markets_df)

    # Step 4: Compute calibration & forecast error
    print("\nSTEP 4: Computing calibration analysis...")
    calibration_df = compute_calibration(markets_df)
    if calibration_df is None or (isinstance(calibration_df, pd.DataFrame) and calibration_df.empty):
        calibration_df = pd.DataFrame()

    # Step 5: Identify opportunities
    print("\nSTEP 5: Identifying mispriced markets & opportunities...")
    opportunities_df = identify_mispriced_markets(markets_df, prices_df)
    if opportunities_df is None:
        opportunities_df = pd.DataFrame()

    stale_markets = detect_slow_reactions(prices_df)
    biases = detect_seasonal_biases(markets_df)

    # Step 6: Generate summary
    print("\nSTEP 6: Generating summary...")
    summary = generate_summary(
        markets_df, prices_df, calibration_df,
        opportunities_df, stale_markets, biases,
    )

    print("\n" + summary)

    print(f"\n{'='*60}")
    print(f"SESSION COMPLETE: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*60}")

    return summary


if __name__ == "__main__":
    run_session()
