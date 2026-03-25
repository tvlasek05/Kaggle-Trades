#!/usr/bin/env python3
"""
Weather Prediction Market Tracker - Session Runner

Run this script each session to:
1. Pull latest weather prediction market data
2. Append to persistent dataset
3. Verify resolved markets against actual weather
4. Compute calibration and forecast error metrics
5. Identify mispricings, slow reactions, and biases
6. Output a concise summary
"""

import os
import sys
from datetime import datetime, timezone

import pandas as pd

import config
import collector
import weather
import analyzer


def generate_summary(markets_df, price_history_df, outcomes_df, analysis_results):
    """Generate a concise text summary of insights and opportunities."""
    lines = []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines.append(f"=" * 70)
    lines.append(f"WEATHER PREDICTION MARKET TRACKER - Session Summary")
    lines.append(f"Run: {now}")
    lines.append(f"=" * 70)

    # Dataset stats
    lines.append(f"\n--- DATASET STATUS ---")
    lines.append(f"Total markets tracked: {len(markets_df)}")

    active = markets_df[markets_df["resolved"] == False] if not markets_df.empty else pd.DataFrame()
    resolved = markets_df[markets_df["resolved"] == True] if not markets_df.empty else pd.DataFrame()
    lines.append(f"  Active: {len(active)}")
    lines.append(f"  Resolved: {len(resolved)}")
    lines.append(f"Price history snapshots: {len(price_history_df)}")
    lines.append(f"Verified outcomes: {len(outcomes_df)}")

    if not markets_df.empty:
        for src in markets_df["source"].unique():
            count = len(markets_df[markets_df["source"] == src])
            lines.append(f"  {src}: {count} markets")

    # Calibration
    cal = analysis_results.get("calibration", pd.DataFrame())
    if not cal.empty:
        lines.append(f"\n--- CALIBRATION ---")
        populated = cal[cal["count"] > 0]
        if not populated.empty:
            for _, row in populated.iterrows():
                err = row.get("calibration_error")
                err_str = f"{err:+.3f}" if err is not None and pd.notna(err) else "N/A"
                lines.append(
                    f"  {row['bucket']}: {int(row['count'])} markets, "
                    f"actual rate={row.get('actual_rate', 'N/A')}, "
                    f"error={err_str}"
                )

            # Overall Brier score
            fe = analysis_results.get("forecast_errors", pd.DataFrame())
            if not fe.empty and "brier_score" in fe.columns:
                brier = fe["brier_score"].mean()
                lines.append(f"  Overall Brier Score: {brier:.4f}")

    # Mispriced markets
    mispriced = analysis_results.get("mispriced", [])
    if mispriced:
        lines.append(f"\n--- POTENTIAL MISPRICINGS ({len(mispriced)}) ---")
        for item in mispriced[:10]:  # Top 10
            lines.append(f"  [{item.get('source', '')}] {item.get('market_id', '')}")
            q = item.get("question", "")
            if q:
                lines.append(f"    Q: {q[:80]}")
            lines.append(f"    Signal: {item.get('signal', '')}")

    # Slow reactions
    slow = analysis_results.get("slow_reactions", [])
    if slow:
        lines.append(f"\n--- SLOW MARKET REACTIONS ({len(slow)}) ---")
        for item in slow[:5]:
            lines.append(f"  [{item.get('source', '')}] {item.get('market_id', '')}")
            lines.append(f"    {item.get('signal', '')}")

    # Biases
    biases = analysis_results.get("biases", [])
    if biases:
        lines.append(f"\n--- BIASES DETECTED ---")
        for b in biases:
            lines.append(f"  [{b.get('type', '')}] {b.get('detail', '')}")

    # Trading opportunities summary
    lines.append(f"\n--- TRADING OPPORTUNITIES ---")
    opportunities = []

    # Illiquid extremes
    illiquid = [m for m in mispriced if m.get("type") == "illiquid_extreme"]
    if illiquid:
        opportunities.append(
            f"  {len(illiquid)} thin markets with extreme prices (potential arb)"
        )

    # Large moves
    movers = [m for m in mispriced if m.get("type") == "large_move"]
    if movers:
        opportunities.append(
            f"  {len(movers)} markets with large recent price moves (momentum/reversal)"
        )

    # Slow drifts
    if slow:
        opportunities.append(
            f"  {len(slow)} markets with consistent drift (may continue)"
        )

    # Calibration-based
    if not cal.empty:
        over = cal[(cal["calibration_error"].notna()) & (cal["calibration_error"] < -0.1)]
        under = cal[(cal["calibration_error"].notna()) & (cal["calibration_error"] > 0.1)]
        if not over.empty:
            opportunities.append(
                f"  Markets in buckets {list(over['bucket'].values)} are OVERPRICED (actual < implied)"
            )
        if not under.empty:
            opportunities.append(
                f"  Markets in buckets {list(under['bucket'].values)} are UNDERPRICED (actual > implied)"
            )

    if opportunities:
        for opp in opportunities:
            lines.append(opp)
    else:
        lines.append("  No clear opportunities identified this session.")
        lines.append("  (More data needed - run more sessions to build history)")

    lines.append(f"\n{'=' * 70}")

    summary = "\n".join(lines)
    return summary


def main():
    """Run a complete session."""
    demo_mode = "--demo" in sys.argv
    print("Starting weather prediction market tracker session...\n")
    if demo_mode:
        print("*** DEMO MODE: Using generated sample data ***\n")

    # Ensure directories exist
    os.makedirs(config.DATA_DIR, exist_ok=True)
    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    if demo_mode:
        # Generate and use demo data
        import demo_data
        print("=" * 50)
        print("STEP 1: Generating demo market data")
        print("=" * 50)
        demo_data.seed_demo_data()
    else:
        # Step 1: Collect market data from live APIs
        print("=" * 50)
        print("STEP 1: Collecting prediction market data")
        print("=" * 50)
        collector.collect_all()

    # Step 2: Load persistent data
    print("\n" + "=" * 50)
    print("STEP 2: Loading persistent dataset")
    print("=" * 50)
    markets_df = collector.load_existing_markets()
    price_history_df = collector.load_price_history()

    # Load or initialize outcomes
    outcomes_df = pd.DataFrame()
    if os.path.exists(config.OUTCOMES_FILE):
        outcomes_df = pd.read_csv(config.OUTCOMES_FILE)
    print(f"  Markets: {len(markets_df)}, Prices: {len(price_history_df)}, Outcomes: {len(outcomes_df)}")

    if not demo_mode:
        # Step 3: Verify resolved markets
        print("\n" + "=" * 50)
        print("STEP 3: Verifying resolved markets against weather data")
        print("=" * 50)
        new_outcomes = weather.verify_resolved_markets(markets_df)
        if not new_outcomes.empty:
            outcomes_df = new_outcomes

        # Step 4: Get current forecasts for active markets
        print("\n" + "=" * 50)
        print("STEP 4: Fetching weather forecasts for active markets")
        print("=" * 50)
        forecasts = weather.get_current_conditions_for_markets(markets_df)
        print(f"  Retrieved forecasts for {len(forecasts)} cities")
    else:
        forecasts = {}

    # Step 5: Run analysis
    print("\n" + "=" * 50)
    print("STEP 5: Running analysis")
    print("=" * 50)
    analysis_results = analyzer.run_analysis(
        markets_df, price_history_df, outcomes_df, forecasts
    )

    # Step 6: Generate and save summary
    print("\n" + "=" * 50)
    print("STEP 6: Generating summary")
    print("=" * 50)
    summary = generate_summary(markets_df, price_history_df, outcomes_df, analysis_results)

    # Save summary
    with open(config.SUMMARY_FILE, "w") as f:
        f.write(summary)
    print(f"  Summary saved to {config.SUMMARY_FILE}")

    # Print summary to console
    print("\n" + summary)

    return 0


if __name__ == "__main__":
    sys.exit(main())
