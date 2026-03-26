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
    python main.py --commit     # Run full pipeline and git commit results
"""

import argparse
import subprocess
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
    try:
        markets = fetch_markets.run()
    except Exception as e:
        print(f"  Kalshi fetch failed: {e}")
        markets = fetch_markets.load_existing_markets()

    print()
    print("=" * 60)
    print("STEP 1b: Fetching supplementary markets (Polymarket, Manifold)")
    print("=" * 60)
    try:
        supplementary = fetch_polymarket.run()
    except Exception as e:
        print(f"  Supplementary fetch failed: {e}")
        supplementary = {}

    print()
    print("=" * 60)
    print("STEP 2: Fetching actual weather outcomes from Open-Meteo")
    print("=" * 60)
    try:
        outcomes = fetch_weather.run(markets)
    except Exception as e:
        print(f"  Weather fetch failed: {e}")
        outcomes = fetch_weather.load_existing_outcomes()

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


def git_commit_results():
    """Stage and commit data/output files if there are changes."""
    files_to_add = [
        "data/markets.csv", "data/prices.csv", "data/outcomes.csv",
        "data/polymarket.csv", "data/manifold.csv",
        "output/analysis.csv", "output/summary.txt", "output/last_run.txt",
    ]
    # Only add files that exist
    existing = [f for f in files_to_add if (config.BASE_DIR / f).exists()]
    if not existing:
        print("\nNo data files to commit.")
        return

    subprocess.run(["git", "add"] + existing, cwd=config.BASE_DIR, check=True)

    # Check if there are staged changes
    result = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        cwd=config.BASE_DIR,
    )
    if result.returncode == 0:
        print("\nNo changes to commit.")
        return

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    msg = f"data: update weather market data ({now})"
    subprocess.run(["git", "commit", "-m", msg], cwd=config.BASE_DIR, check=True)
    print(f"\nCommitted: {msg}")


def main():
    parser = argparse.ArgumentParser(description="Weather Prediction Market Analysis")
    parser.add_argument("--fetch", action="store_true", help="Only fetch market data")
    parser.add_argument("--weather", action="store_true", help="Only fetch weather outcomes")
    parser.add_argument("--analyze", action="store_true", help="Only run analysis")
    parser.add_argument("--commit", action="store_true", help="Run full pipeline and git commit results")
    args = parser.parse_args()

    if args.commit:
        run_full_pipeline()
        git_commit_results()
    elif not any([args.fetch, args.weather, args.analyze]):
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
