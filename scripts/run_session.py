#!/usr/bin/env python3
"""
Main session orchestrator for weather prediction market analysis.

Each run:
1. Pulls latest weather prediction market data from Polymarket/Kalshi
2. Appends new data to persistent CSV dataset
3. Fetches actual weather outcomes from Open-Meteo
4. Identifies resolved markets and records results
5. Runs full analysis (calibration, errors, mispricings, biases)
6. Outputs a concise summary of insights and trading opportunities
"""

import json
import os
import sys
from datetime import datetime, timezone

# Add parent directory to path
sys.path.insert(0, os.path.dirname(__file__))

from fetch_markets import fetch_all_weather_markets
from fetch_weather import fetch_all_city_weather
from storage import (
    append_market_snapshots,
    append_weather_actuals,
    append_resolved_markets,
    load_market_snapshots,
    load_resolved_markets,
    save_analysis_results,
)
from analysis import run_full_analysis, format_summary


def run_session():
    """Execute one full data collection and analysis session."""
    print(f"\n{'='*60}")
    print(f"SESSION START: {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}\n")

    # Step 1: Fetch prediction market data
    print("[1/5] Fetching prediction market data...")
    markets = fetch_all_weather_markets()

    # Separate active vs resolved
    active_markets = [m for m in markets if not m.get("resolved")]
    resolved_markets = [m for m in markets if m.get("resolved")]

    # Mark resolution as boolean for resolved markets
    for m in resolved_markets:
        res = m.get("resolution")
        m["resolved_yes"] = res == "Yes" or res == "1" or res is True

    print(f"  Active markets: {len(active_markets)}")
    print(f"  Resolved markets: {len(resolved_markets)}")

    # Step 2: Store market snapshots
    print("\n[2/5] Storing market snapshots...")
    n_stored = append_market_snapshots(active_markets + resolved_markets)
    print(f"  Stored {n_stored} market snapshots")

    n_resolved_stored = append_resolved_markets(resolved_markets)
    print(f"  Stored {n_resolved_stored} resolved market records")

    # Step 3: Fetch actual weather data
    print("\n[3/5] Fetching actual weather data from Open-Meteo...")
    weather_records = fetch_all_city_weather(lookback_days=14)
    n_weather = append_weather_actuals(weather_records)
    print(f"  Stored {n_weather} weather records")

    # Step 4: Run analysis
    print("\n[4/5] Running analysis...")
    df_snapshots = load_market_snapshots()
    df_resolved = load_resolved_markets()
    results = run_full_analysis(df_snapshots, df_resolved)

    # Step 5: Save results and output summary
    print("\n[5/5] Saving results and generating summary...")
    analysis_path = save_analysis_results(
        [results], filename="latest_analysis"
    )

    # Save detailed results as JSON
    json_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "data", "latest_analysis.json"
    )
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    summary = format_summary(results)
    print(f"\n{summary}")

    # Save summary text
    summary_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "data", "latest_summary.txt"
    )
    with open(summary_path, "w") as f:
        f.write(summary)

    print(f"\nFiles saved:")
    print(f"  Analysis: {analysis_path}")
    print(f"  JSON:     {json_path}")
    print(f"  Summary:  {summary_path}")

    print(f"\nSESSION COMPLETE: {datetime.now(timezone.utc).isoformat()}")
    return results


if __name__ == "__main__":
    run_session()
