#!/usr/bin/env python3
"""
Weather Prediction Market Analysis Pipeline

Pulls weather prediction market data from Kalshi, fetches actual weather
outcomes from Open-Meteo, and runs calibration/mispricing analysis.

Usage:
    python main.py              # Run full pipeline
    python main.py --fetch      # Only fetch new market data
    python main.py --weather    # Only fetch weather outcomes
    python main.py --analyze    # Only run analysis
"""

import argparse
import sys
from datetime import datetime, timezone

import fetch_markets
import fetch_polymarket
import fetch_weather
import analyze
import config


def run_full_pipeline():
    """Execute the complete pipeline: fetch markets, fetch weather, analyze.

    Gracefully degrades when APIs are unreachable — always runs analysis
    on whatever data is available (freshly fetched or previously stored).
    """
    api_errors = []

    print("=" * 60)
    print("STEP 1: Fetching weather prediction market data from Kalshi")
    print("=" * 60)
    try:
        markets = fetch_markets.run()
    except Exception as e:
        print(f"  [WARN] Kalshi fetch failed: {e}")
        api_errors.append(f"Kalshi: {e}")
        markets = fetch_markets.load_existing_markets()
        print(f"  Loaded {len(markets)} markets from existing data")

    print()
    print("=" * 60)
    print("STEP 1b: Fetching supplementary markets (Polymarket, Manifold)")
    print("=" * 60)
    try:
        supplementary = fetch_polymarket.run()
    except Exception as e:
        print(f"  [WARN] Supplementary fetch failed: {e}")
        api_errors.append(f"Supplementary: {e}")

    print()
    print("=" * 60)
    print("STEP 2: Fetching actual weather outcomes from Open-Meteo")
    print("=" * 60)
    try:
        outcomes = fetch_weather.run(markets)
    except Exception as e:
        print(f"  [WARN] Weather fetch failed: {e}")
        api_errors.append(f"Open-Meteo: {e}")
        outcomes = fetch_weather.load_existing_outcomes()
        print(f"  Loaded {len(outcomes)} outcomes from existing data")

    print()
    print("=" * 60)
    print("STEP 3: Running analysis")
    print("=" * 60)
    summary = analyze.run(markets, outcomes)

    # Write a run log
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    log_path = config.OUTPUT_DIR / "last_run.txt"
    with open(log_path, "w") as f:
        f.write(f"Last run: {now}\n")
        f.write(f"Markets tracked: {len(markets)}\n")
        f.write(f"Outcomes recorded: {len(outcomes)}\n")
        if api_errors:
            f.write(f"API errors: {'; '.join(api_errors)}\n")

    return summary


def main():
    parser = argparse.ArgumentParser(description="Weather Prediction Market Analysis")
    parser.add_argument("--fetch", action="store_true", help="Only fetch market data")
    parser.add_argument("--weather", action="store_true", help="Only fetch weather outcomes")
    parser.add_argument("--analyze", action="store_true", help="Only run analysis")
    args = parser.parse_args()

    if not any([args.fetch, args.weather, args.analyze]):
        run_full_pipeline()
    else:
        if args.fetch:
            fetch_markets.run()
        if args.weather:
            fetch_weather.run()
        if args.analyze:
            analyze.run()


if __name__ == "__main__":
    main()
