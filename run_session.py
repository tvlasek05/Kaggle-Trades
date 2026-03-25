#!/usr/bin/env python3
"""
Main session runner for weather prediction market tracking.

Each run:
1. Pulls latest weather prediction market data from Polymarket and Kalshi
2. Appends new data to persistent CSV dataset
3. Fetches actual weather outcomes from Open-Meteo
4. Runs analysis: calibration, forecast error, mispricing, biases
5. Saves updated dataset and analysis results
6. Outputs concise summary

If network is unavailable (e.g., sandboxed environment), falls back to
seed data so the full analysis pipeline can still demonstrate functionality.
"""

import sys
import os

# Ensure imports work from project root
sys.path.insert(0, os.path.dirname(__file__))

from market_fetcher import fetch_all_weather_markets, append_market_snapshots
from weather_actuals import update_weather_actuals
from analysis import (
    load_prices,
    load_actuals,
    generate_analysis_report,
    save_report,
    format_summary,
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
MARKET_PRICES_FILE = os.path.join(DATA_DIR, "market_prices.csv")


def ensure_data_exists():
    """If no data exists yet, generate seed data."""
    if not os.path.exists(MARKET_PRICES_FILE):
        print("  No existing data found. Generating seed dataset...")
        from seed_data import seed
        seed()
        return True
    return False


def run_session():
    print("=" * 60)
    print("WEATHER PREDICTION MARKET SESSION")
    print("=" * 60)

    # Ensure we have data to work with
    seeded = ensure_data_exists()

    # Step 1: Fetch prediction market data (live)
    print("\n[1/4] Fetching prediction market data...")
    try:
        markets = fetch_all_weather_markets()
        if markets:
            n_prices = append_market_snapshots(markets)
            print(f"  -> {len(markets)} markets fetched, {n_prices} price snapshots saved")
        else:
            print("  -> No live markets fetched (API may be unavailable)")
            if seeded:
                print("  -> Using seed data for analysis")
    except Exception as e:
        print(f"  -> Live fetch failed: {e}")
        print("  -> Continuing with existing data")

    # Step 2: Fetch actual weather data
    print("\n[2/4] Fetching weather actuals...")
    try:
        n_actuals = update_weather_actuals(days_back=14)
        if n_actuals > 0:
            print(f"  -> {n_actuals} weather records updated")
        else:
            print("  -> No new weather data (API may be unavailable)")
    except Exception as e:
        print(f"  -> Weather fetch failed: {e}")
        print("  -> Continuing with existing data")

    # Step 3: Run analysis
    print("\n[3/4] Running analysis...")
    prices_df = load_prices()
    actuals_df = load_actuals()

    if prices_df.empty:
        print("  -> No price data available. Cannot run analysis.")
        return None

    report = generate_analysis_report(prices_df, actuals_df)

    # Step 4: Save and display results
    print("\n[4/4] Saving results...")
    report_path = save_report(report)
    print(f"  -> Report saved to {report_path}")

    # Print summary
    print("\n")
    print(format_summary(report))

    return report


if __name__ == "__main__":
    run_session()
