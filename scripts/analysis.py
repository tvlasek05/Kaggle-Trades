"""
Analysis module for weather prediction market data.

Computes:
- Implied probability vs actual outcome
- Forecast error metrics
- Calibration by probability bucket
- Mispriced markets detection
- Slow market reaction identification
- Seasonal/regional bias analysis
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone


def compute_calibration(df_resolved, n_buckets=10):
    """
    Compute calibration: for each probability bucket, what fraction resolved YES?

    Args:
        df_resolved: DataFrame with 'price_yes' (implied probability) and
                     'resolved_yes' (bool: did it resolve YES?)
    Returns:
        DataFrame with bucket ranges, mean implied prob, actual frequency, and count.
    """
    if df_resolved.empty or "price_yes" not in df_resolved.columns:
        return pd.DataFrame()

    df = df_resolved.dropna(subset=["price_yes", "resolved_yes"]).copy()
    if df.empty:
        return pd.DataFrame()

    df["prob_bucket"] = pd.cut(
        df["price_yes"], bins=n_buckets, labels=False, include_lowest=True
    )

    buckets = []
    for bucket_id in sorted(df["prob_bucket"].dropna().unique()):
        group = df[df["prob_bucket"] == bucket_id]
        bucket_low = group["price_yes"].min()
        bucket_high = group["price_yes"].max()
        mean_implied = group["price_yes"].mean()
        actual_freq = group["resolved_yes"].mean()
        count = len(group)

        buckets.append({
            "bucket": f"{bucket_low:.2f}-{bucket_high:.2f}",
            "mean_implied_prob": round(mean_implied, 4),
            "actual_frequency": round(actual_freq, 4),
            "calibration_error": round(abs(mean_implied - actual_freq), 4),
            "count": count,
        })

    return pd.DataFrame(buckets)


def compute_forecast_errors(df_resolved):
    """
    Compute forecast error metrics for resolved markets.

    Returns: dict with Brier score, log loss, and mean absolute error.
    """
    if df_resolved.empty:
        return {}

    df = df_resolved.dropna(subset=["price_yes", "resolved_yes"]).copy()
    if df.empty:
        return {}

    probs = df["price_yes"].values
    outcomes = df["resolved_yes"].astype(float).values

    # Brier score
    brier = np.mean((probs - outcomes) ** 2)

    # Log loss (clip to avoid log(0))
    eps = 1e-7
    probs_clipped = np.clip(probs, eps, 1 - eps)
    log_loss = -np.mean(
        outcomes * np.log(probs_clipped) + (1 - outcomes) * np.log(1 - probs_clipped)
    )

    # Mean absolute error
    mae = np.mean(np.abs(probs - outcomes))

    return {
        "brier_score": round(float(brier), 6),
        "log_loss": round(float(log_loss), 6),
        "mean_absolute_error": round(float(mae), 6),
        "n_resolved": len(df),
    }


def detect_mispriced_markets(df_snapshots):
    """
    Identify potentially mispriced markets based on:
    - Extreme prices close to expiry (0.1-0.9 range near expiry suggests uncertainty)
    - Large price swings in short periods
    - Low liquidity markets with stale prices
    """
    if df_snapshots.empty:
        return []

    mispriced = []

    # Group by market_id
    for market_id, group in df_snapshots.groupby("market_id"):
        if len(group) < 1:
            continue

        latest = group.sort_values("fetch_timestamp").iloc[-1]
        price = latest.get("price_yes")
        if price is None or pd.isna(price):
            continue

        signals = []

        # Signal 1: Mid-range price with approaching expiry
        end_date = latest.get("end_date", "")
        if end_date:
            try:
                expiry = pd.to_datetime(end_date, utc=True)
                now = pd.Timestamp.now(tz="UTC")
                days_to_expiry = (expiry - now).days
                if 0 < days_to_expiry <= 3 and 0.2 < price < 0.8:
                    signals.append(
                        f"Uncertain price ({price:.2f}) with only {days_to_expiry}d to expiry"
                    )
            except (ValueError, TypeError):
                pass

        # Signal 2: Large recent price movement
        if len(group) >= 2:
            sorted_group = group.sort_values("fetch_timestamp")
            prices = sorted_group["price_yes"].dropna()
            if len(prices) >= 2:
                price_change = abs(prices.iloc[-1] - prices.iloc[-2])
                if price_change > 0.15:
                    signals.append(
                        f"Large price swing: {price_change:.2f} between snapshots"
                    )

        # Signal 3: Very low liquidity
        liquidity = latest.get("liquidity", 0)
        if isinstance(liquidity, (int, float)) and liquidity < 1000 and price is not None:
            signals.append(f"Low liquidity ({liquidity}), price may be stale")

        if signals:
            mispriced.append({
                "market_id": market_id,
                "source": latest.get("source", ""),
                "question": latest.get("question", ""),
                "current_price": price,
                "signals": "; ".join(signals),
                "end_date": end_date,
                "liquidity": liquidity,
            })

    return mispriced


def detect_slow_reactions(df_snapshots, threshold_hours=6):
    """
    Identify markets that are slow to react to new information.
    Looks for markets where price hasn't updated despite time passing.
    """
    if df_snapshots.empty or len(df_snapshots) < 2:
        return []

    slow = []
    for market_id, group in df_snapshots.groupby("market_id"):
        sorted_group = group.sort_values("fetch_timestamp")
        if len(sorted_group) < 2:
            continue

        prices = sorted_group["price_yes"].dropna()
        if len(prices) < 2:
            continue

        # Check if price is stale (hasn't moved across multiple snapshots)
        unique_prices = prices.nunique()
        if unique_prices == 1 and len(prices) >= 3:
            latest = sorted_group.iloc[-1]
            slow.append({
                "market_id": market_id,
                "source": latest.get("source", ""),
                "question": latest.get("question", ""),
                "stale_price": float(prices.iloc[-1]),
                "snapshots_unchanged": len(prices),
                "signal": "Price unchanged across multiple snapshots - may be slow to react",
            })

    return slow


def analyze_regional_seasonal_bias(df_resolved):
    """
    Look for systematic biases by region or season in resolved markets.
    """
    if df_resolved.empty:
        return {}

    df = df_resolved.dropna(subset=["price_yes", "resolved_yes"]).copy()
    if df.empty:
        return {}

    results = {"by_source": {}}

    # Bias by source
    for source, group in df.groupby("source"):
        if len(group) >= 3:
            mean_prob = group["price_yes"].mean()
            actual_rate = group["resolved_yes"].mean()
            results["by_source"][source] = {
                "mean_implied_prob": round(float(mean_prob), 4),
                "actual_resolution_rate": round(float(actual_rate), 4),
                "bias": round(float(mean_prob - actual_rate), 4),
                "count": len(group),
            }

    # Check for keywords suggesting seasonal patterns
    seasonal_keywords = {
        "summer": ["heat", "hot", "warm", "summer"],
        "winter": ["cold", "snow", "freeze", "winter", "ice"],
        "storm": ["hurricane", "tornado", "storm", "flood"],
    }

    results["by_category"] = {}
    for category, keywords in seasonal_keywords.items():
        mask = df["question"].str.lower().str.contains("|".join(keywords), na=False)
        subset = df[mask]
        if len(subset) >= 2:
            mean_prob = subset["price_yes"].mean()
            actual_rate = subset["resolved_yes"].mean()
            results["by_category"][category] = {
                "mean_implied_prob": round(float(mean_prob), 4),
                "actual_resolution_rate": round(float(actual_rate), 4),
                "bias": round(float(mean_prob - actual_rate), 4),
                "count": len(subset),
            }

    return results


def run_full_analysis(df_snapshots, df_resolved):
    """Run all analyses and return a structured results dict."""
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "snapshot_count": len(df_snapshots),
        "resolved_count": len(df_resolved),
    }

    # Calibration
    calibration = compute_calibration(df_resolved)
    results["calibration"] = calibration.to_dict("records") if not calibration.empty else []

    # Forecast errors
    results["forecast_errors"] = compute_forecast_errors(df_resolved)

    # Mispriced markets
    results["mispriced_markets"] = detect_mispriced_markets(df_snapshots)

    # Slow reactions
    results["slow_reactions"] = detect_slow_reactions(df_snapshots)

    # Regional/seasonal bias
    results["bias_analysis"] = analyze_regional_seasonal_bias(df_resolved)

    return results


def format_summary(results):
    """Format analysis results into a concise text summary."""
    lines = []
    lines.append("=" * 60)
    lines.append("WEATHER PREDICTION MARKET ANALYSIS SUMMARY")
    lines.append(f"Timestamp: {results.get('timestamp', 'N/A')}")
    lines.append("=" * 60)

    lines.append(f"\nData: {results['snapshot_count']} market snapshots, "
                 f"{results['resolved_count']} resolved markets")

    # Forecast errors
    fe = results.get("forecast_errors", {})
    if fe:
        lines.append(f"\n--- Forecast Accuracy ---")
        lines.append(f"  Brier Score:  {fe.get('brier_score', 'N/A')}")
        lines.append(f"  Log Loss:     {fe.get('log_loss', 'N/A')}")
        lines.append(f"  MAE:          {fe.get('mean_absolute_error', 'N/A')}")
        lines.append(f"  N resolved:   {fe.get('n_resolved', 0)}")

    # Calibration
    cal = results.get("calibration", [])
    if cal:
        lines.append(f"\n--- Calibration by Probability Bucket ---")
        for bucket in cal:
            lines.append(
                f"  [{bucket['bucket']}] implied={bucket['mean_implied_prob']:.3f} "
                f"actual={bucket['actual_frequency']:.3f} "
                f"error={bucket['calibration_error']:.3f} "
                f"(n={bucket['count']})"
            )

    # Mispriced markets
    mispriced = results.get("mispriced_markets", [])
    if mispriced:
        lines.append(f"\n--- Potentially Mispriced Markets ({len(mispriced)}) ---")
        for m in mispriced[:10]:
            lines.append(f"  [{m['source']}] {m['question'][:60]}")
            lines.append(f"    Price: {m['current_price']:.3f} | {m['signals']}")
    else:
        lines.append(f"\n--- No obviously mispriced markets detected ---")

    # Slow reactions
    slow = results.get("slow_reactions", [])
    if slow:
        lines.append(f"\n--- Slow Market Reactions ({len(slow)}) ---")
        for s in slow[:5]:
            lines.append(f"  [{s['source']}] {s['question'][:60]}")
            lines.append(f"    Stale at {s['stale_price']:.3f} for {s['snapshots_unchanged']} snapshots")

    # Bias
    bias = results.get("bias_analysis", {})
    if bias.get("by_source"):
        lines.append(f"\n--- Bias by Source ---")
        for source, stats in bias["by_source"].items():
            direction = "overconfident" if stats["bias"] > 0 else "underconfident"
            lines.append(
                f"  {source}: {direction} by {abs(stats['bias']):.3f} "
                f"(implied={stats['mean_implied_prob']:.3f}, "
                f"actual={stats['actual_resolution_rate']:.3f}, n={stats['count']})"
            )
    if bias.get("by_category"):
        lines.append(f"\n--- Bias by Weather Category ---")
        for cat, stats in bias["by_category"].items():
            direction = "overconfident" if stats["bias"] > 0 else "underconfident"
            lines.append(
                f"  {cat}: {direction} by {abs(stats['bias']):.3f} "
                f"(n={stats['count']})"
            )

    # Trading opportunities
    lines.append(f"\n--- Trading Opportunities ---")
    opportunities = []
    for m in mispriced:
        if m.get("current_price") and m["current_price"] < 0.15:
            opportunities.append(f"  BUY YES: {m['question'][:50]} @ {m['current_price']:.3f}")
        elif m.get("current_price") and m["current_price"] > 0.85:
            opportunities.append(f"  BUY NO: {m['question'][:50]} @ {1-m['current_price']:.3f}")

    if opportunities:
        for opp in opportunities[:5]:
            lines.append(opp)
    else:
        lines.append("  No clear opportunities identified this session.")
        lines.append("  (More data needed for robust signal generation)")

    lines.append("\n" + "=" * 60)
    return "\n".join(lines)
