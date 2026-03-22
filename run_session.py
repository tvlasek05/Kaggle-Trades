#!/usr/bin/env python3
"""
Weather Prediction Market Tracker - Session Runner

Each execution:
1. Pulls latest weather prediction market data (Kalshi + Polymarket)
2. Fetches actual weather data from Open-Meteo
3. Appends to persistent CSV datasets
4. Matches resolved markets with actual outcomes
5. Runs calibration and mispricing analysis
6. Saves results and prints summary
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

import pandas as pd

import config
from fetch_markets import fetch_all_markets
from fetch_weather import fetch_all_weather_actuals
from analyze import run_full_analysis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def ensure_data_dir():
    os.makedirs(config.DATA_DIR, exist_ok=True)


def append_csv(filepath, new_rows, dedup_cols=None):
    """Append rows to a CSV, optionally deduplicating."""
    new_df = pd.DataFrame(new_rows)
    if new_df.empty:
        return pd.DataFrame()

    try:
        existing = pd.read_csv(filepath)
    except FileNotFoundError:
        existing = pd.DataFrame()

    combined = pd.concat([existing, new_df], ignore_index=True)

    if dedup_cols and all(c in combined.columns for c in dedup_cols):
        combined = combined.drop_duplicates(subset=dedup_cols, keep="last")

    combined.to_csv(filepath, index=False)
    new_count = len(combined) - len(existing)
    return combined, new_count


def print_summary(results):
    """Print a concise session summary."""
    print("\n" + "=" * 70)
    print("  WEATHER PREDICTION MARKET TRACKER - SESSION SUMMARY")
    print("  " + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    print("=" * 70)

    ds = results.get("data_summary", {})
    print(f"\n📊 Dataset: {ds.get('total_snapshots', 0)} snapshots across "
          f"{ds.get('unique_markets', 0)} markets")
    print(f"   Sources: {ds.get('sources', {})}")
    print(f"   Resolved markets: {ds.get('resolved_markets', 0)}")
    print(f"   Weather data rows: {ds.get('weather_rows', 0)}")

    # Forecast accuracy
    fe = results.get("forecast_errors", {})
    if fe:
        print(f"\n🎯 Forecast Accuracy ({fe.get('n_markets', 0)} resolved markets):")
        print(f"   Mean Brier Score: {fe.get('mean_brier_score', 'N/A')}")
        print(f"   Mean Log Loss:    {fe.get('mean_log_loss', 'N/A')}")
        if fe.get("brier_by_series"):
            for series, score in fe["brier_by_series"].items():
                print(f"   - {series}: {score}")

    # Calibration
    cal = results.get("calibration", [])
    if cal:
        print(f"\n📐 Calibration ({len(cal)} buckets with data):")
        for bucket in cal:
            err = bucket.get("calibration_error", 0)
            direction = "▲ underconfident" if err > 0.03 else "▼ overconfident" if err < -0.03 else "✓"
            print(f"   [{bucket['prob_bucket']}] implied={bucket['mean_implied_prob']:.2f} "
                  f"actual={bucket['actual_frequency']:.2f} (n={bucket['count']}) {direction}")

    # Mispriced markets
    opp = results.get("mispriced_markets", [])
    if opp:
        print(f"\n💰 Potential Mispriced Markets ({len(opp)} found):")
        for o in opp[:10]:
            print(f"   {o['signal']}: {o['market_id']}")
            print(f"     {o['title']} | {o['city']} {o['date']}")
            print(f"     Strike: {o['strike_f']}°F | Forecast: {o['forecast_high_f']}°F")
            print(f"     Market: {o['implied_prob']:.0%} | Model: {o['model_prob']:.0%} | Edge: {o['edge']:+.1%}")
    else:
        print("\n💰 No significantly mispriced markets detected.")

    # Slow reactions
    slow = results.get("slow_reactions", [])
    if slow:
        print(f"\n⏱️  Slow Market Reactions ({len(slow)} detected):")
        for s in slow[:5]:
            print(f"   {s['market_id']}: {s['price_range']:.0%} move over {s['hours_span']:.0f}h")

    # Regional biases
    rb = results.get("regional_biases", {})
    if rb:
        print(f"\n🌍 Regional Biases:")
        for city, stats in rb.items():
            print(f"   {city}: bias={stats['mean_bias']:+.3f} ({stats['bias_direction']}, n={stats['n_markets']})")

    # Seasonal biases
    sb = results.get("seasonal_biases", {})
    if sb:
        print(f"\n📅 Seasonal Biases:")
        for month, stats in sb.items():
            print(f"   {month}: brier={stats['mean_brier']:.3f} bias={stats['mean_bias']:+.3f} (n={stats['n_markets']})")

    print("\n" + "=" * 70)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Weather Prediction Market Tracker")
    parser.add_argument("--offline", action="store_true",
                        help="Skip API fetches, analyze existing data only")
    args = parser.parse_args()

    logger.info("Starting weather prediction market session...")
    ensure_data_dir()

    if not args.offline:
        # Step 1: Fetch market data
        print("Fetching market data...")
        market_rows = fetch_all_markets()
        if market_rows:
            combined, new_count = append_csv(
                config.MARKET_SNAPSHOTS_FILE,
                market_rows,
            )
            logger.info("Market snapshots: %d new rows added (total: %d)", new_count, len(combined))
        else:
            logger.warning("No market data fetched. APIs may be unavailable.")

        # Step 2: Fetch weather actuals
        print("Fetching weather data...")
        weather_rows = fetch_all_weather_actuals()
        if weather_rows:
            combined, new_count = append_csv(
                config.WEATHER_ACTUALS_FILE,
                weather_rows,
                dedup_cols=["date", "city"],
            )
            logger.info("Weather actuals: %d new rows (total: %d)", new_count, len(combined))
        else:
            logger.warning("No weather data fetched.")
    else:
        logger.info("Offline mode: skipping API fetches, using existing data.")

    # Step 3: Run analysis
    print("Running analysis...")
    results = run_full_analysis()

    # Step 4: Save analysis results
    with open(config.ANALYSIS_FILE, "w") as f:
        json.dump(results, f, indent=2, default=str)
    logger.info("Analysis saved to %s", config.ANALYSIS_FILE)

    # Step 5: Print summary
    print_summary(results)

    return 0


if __name__ == "__main__":
    sys.exit(main())
