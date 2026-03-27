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
    """Execute the complete pipeline: fetch markets, fetch weather, analyze."""
    print("=" * 60)
    print("STEP 1: Fetching weather prediction market data from Kalshi")
    print("=" * 60)
    markets = fetch_markets.run()

    print()
    print("=" * 60)
    print("STEP 1b: Fetching supplementary markets (Polymarket, Manifold)")
    print("=" * 60)
    supplementary = fetch_polymarket.run()

    print()
    print("=" * 60)
    print("STEP 2: Fetching actual weather outcomes from Open-Meteo")
    print("=" * 60)
    outcomes = fetch_weather.run(markets)

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

    return summary


def main():
    parser = argparse.ArgumentParser(description="Weather Prediction Market Analysis")
    parser.add_argument("--fetch", action="store_true", help="Only fetch market data")
    parser.add_argument("--weather", action="store_true", help="Only fetch weather outcomes")
    parser.add_argument("--analyze", action="store_true", help="Only run analysis")
    parser.add_argument("--commit", action="store_true", help="Git commit data after pipeline run")
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

    if args.commit:
        import subprocess
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        msg = f"data: update weather market data ({now})"
        subprocess.run(["git", "add", "data/", "output/"], check=False)
        result = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if result.returncode != 0:
            subprocess.run(["git", "commit", "-m", msg], check=True)
            print(f"\nCommitted: {msg}")
        else:
            print("\nNo data changes to commit.")


if __name__ == "__main__":
    main()
