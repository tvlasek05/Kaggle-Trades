#!/usr/bin/env python3
"""
Weather Prediction Market Tracker & Analyzer

Each session:
1. Pull latest weather prediction market data from Polymarket and Kalshi
2. Append new data to persistent CSV datasets
3. Maintain rolling time series of prices
4. Fetch actual weather outcomes for resolved markets
5. Compute calibration, forecast error, and bias analysis
6. Identify mispriced markets and trading opportunities
7. Save all results and output summary
"""

import sys
import logging
from datetime import datetime, timezone

import config
import fetch_markets
import fetch_weather
import dataset
import analysis
import seed_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(config.BASE_DIR / "session.log"),
    ],
)
logger = logging.getLogger(__name__)


def run_session():
    """Execute a full data collection and analysis session."""
    session_start = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info("Session started at %s", session_start.isoformat())
    logger.info("=" * 60)

    # -- Step 0: Seed data if empty --
    print("\n[0/6] Checking dataset...")
    seed_data.seed_if_empty()

    # -- Step 1: Fetch latest market data --
    print("\n[1/6] Fetching weather prediction markets...")
    new_markets = fetch_markets.fetch_all_weather_markets()
    if new_markets.empty:
        print("  -> No new markets fetched from APIs (may be unavailable).")
        print("  -> Using existing dataset.")
        new_markets_count = 0
    else:
        new_markets_count = len(new_markets)
        print(f"  -> Fetched {new_markets_count} weather markets")

    # -- Step 2: Update persistent market dataset --
    print("\n[2/6] Updating market dataset...")
    if not new_markets.empty:
        markets_df = dataset.update_markets(new_markets)
    else:
        markets_df = dataset.load_markets()
    print(f"  -> Total markets in dataset: {len(markets_df) if not markets_df.empty else 0}")

    # -- Step 3: Append price snapshots --
    print("\n[3/6] Recording price snapshots...")
    if not new_markets.empty:
        snapshots_df = dataset.append_price_snapshots(new_markets)
        print(f"  -> Total price snapshots: {len(snapshots_df)}")
    else:
        snapshots_df = dataset.load_snapshots()
        print(f"  -> Using existing snapshots: {len(snapshots_df) if not snapshots_df.empty else 0}")

    # -- Step 4: Fetch weather outcomes for resolved markets --
    print("\n[4/6] Fetching actual weather outcomes for resolved markets...")
    outcomes_list = fetch_weather.fetch_outcomes_for_markets(markets_df)
    outcomes_df = dataset.update_outcomes(outcomes_list)
    print(f"  -> New outcomes fetched: {len(outcomes_list)}")
    print(f"  -> Total outcomes in dataset: {len(outcomes_df) if not outcomes_df.empty else 0}")

    # -- Step 5: Run analysis --
    print("\n[5/6] Running analysis...")
    results = analysis.run_full_analysis(markets_df, snapshots_df, outcomes_df)

    # -- Step 6: Output summary --
    print("\n[6/6] Session Summary")
    print("=" * 60)
    _print_summary(results)

    session_end = datetime.now(timezone.utc)
    duration = (session_end - session_start).total_seconds()
    logger.info("Session completed in %.1f seconds", duration)
    print(f"\nSession completed in {duration:.1f}s")
    print(f"Data saved to: {config.DATA_DIR}")
    print(f"Analysis saved to: {config.ANALYSIS_RESULTS}")

    return results


def _print_summary(results: dict):
    """Print a concise summary of analysis results."""
    mc = results.get("market_counts", {})
    print(f"\nMarket Overview:")
    print(f"  Total markets tracked: {mc.get('total', 0)}")
    print(f"  Active: {mc.get('active', 0)}")
    print(f"  Resolved: {mc.get('resolved', 0)}")
    by_source = mc.get("by_source", {})
    for src, count in by_source.items():
        print(f"    {src}: {count}")

    # Calibration
    cal = results.get("calibration", {})
    buckets = cal.get("buckets", [])
    if buckets:
        print(f"\nCalibration Analysis ({cal.get('total_resolved_markets', 0)} resolved markets):")
        print(f"  Mean calibration error: {cal.get('mean_calibration_error', 0):.4f}")
        print(f"  {'Bucket':<12} {'Count':>6} {'Implied':>8} {'Actual':>8} {'Error':>8}")
        for b in buckets:
            print(f"  {b['range']:<12} {b['count']:>6} {b['implied_prob_avg']:>8.3f} {b['actual_rate']:>8.3f} {b['calibration_error']:>8.4f}")

    # Forecast errors
    fe = results.get("forecast_errors", {})
    if fe.get("count"):
        print(f"\nForecast Error Metrics:")
        print(f"  Brier score: {fe.get('brier_score', 0):.4f}")
        print(f"  Mean absolute error: {fe.get('mean_absolute_error', 0):.4f}")
        print(f"  Mean bias: {fe.get('mean_bias', 0):.4f}")

    # Mispriced markets
    mispriced = results.get("mispriced_markets", [])
    if mispriced:
        print(f"\nPotentially Mispriced Markets ({len(mispriced)}):")
        for mp in mispriced[:10]:  # Top 10
            print(f"  [{mp['source']}] {mp['question'][:70]}")
            print(f"    Yes price: {mp['current_yes_price']:.3f}")
            for sig in mp["signals"]:
                print(f"    -> {sig['type']}: {sig['detail']}")
    else:
        print(f"\nNo obvious mispricing signals detected.")

    # Biases
    biases = results.get("biases", {})
    if biases and not biases.get("note"):
        print(f"\nDetected Biases:")
        for name, info in biases.items():
            if isinstance(info, dict) and info.get("count"):
                print(f"  {name}: mean_error={info.get('mean_error', 0):.4f} ({info.get('direction', 'n/a')}) [n={info['count']}]")

    # Cross-platform divergences
    divs = results.get("cross_platform_divergences", [])
    if divs:
        print(f"\nCross-Platform Price Divergences ({len(divs)}):")
        for d in divs:
            print(f"  {d['city'].title()} (Mar {d['date']}):")
            print(f"    Polymarket Yes: {d['polymarket_yes']:.3f} | Kalshi Yes: {d['kalshi_yes']:.3f} | Spread: {d['spread']:.3f} ({d['direction']})")

    # Trading opportunities summary
    opportunities = []
    if mispriced:
        for mp in mispriced[:5]:
            for sig in mp["signals"]:
                if sig["type"] in ("low_price_high_volume", "thin_liquidity", "large_price_move"):
                    opportunities.append(f"  * [{mp['source']}] {mp['question'][:60]}\n    Signal: {sig['type']}: {sig['detail']}")
                    break
    if divs:
        for d in divs[:3]:
            if d["spread"] >= 0.02:
                opportunities.append(
                    f"  * ARBITRAGE: {d['city'].title()} Mar {d['date']} -- "
                    f"buy {'Kalshi' if d['direction'] == 'Polymarket higher' else 'Polymarket'} Yes @ "
                    f"{min(d['polymarket_yes'], d['kalshi_yes']):.3f}, "
                    f"sell {'Polymarket' if d['direction'] == 'Polymarket higher' else 'Kalshi'} Yes @ "
                    f"{max(d['polymarket_yes'], d['kalshi_yes']):.3f} "
                    f"(spread: {d['spread']:.3f})"
                )

    if opportunities:
        print(f"\nPotential Trading Opportunities:")
        for opp in opportunities:
            print(opp)
    else:
        print(f"\nNo strong trading opportunities identified this session.")


if __name__ == "__main__":
    run_session()
