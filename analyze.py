"""Analyze weather prediction market data: calibration, mispricing, biases."""

import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta

import config


def compute_calibration(markets_df):
    """Compute calibration by probability bucket for resolved markets.

    For each bucket (e.g., 0.2-0.3), what fraction of markets actually resolved YES?
    Perfect calibration: bucket midpoint == actual frequency.

    Returns DataFrame with calibration results.
    """
    resolved = markets_df[markets_df["resolved"] == True].copy()
    if resolved.empty:
        print("No resolved markets for calibration analysis.")
        return pd.DataFrame()

    # Determine actual outcome: 1 if resolved YES, 0 if NO
    resolved["actual_outcome"] = resolved["resolution"].apply(_parse_resolution)
    resolved["implied_prob"] = resolved["outcome_yes_price"].astype(float)

    # Filter out markets where we can't determine outcome
    resolved = resolved[resolved["actual_outcome"].notna()]
    if resolved.empty:
        return pd.DataFrame()

    # Bucket by implied probability
    buckets = config.CALIBRATION_BUCKETS
    resolved["prob_bucket"] = pd.cut(
        resolved["implied_prob"],
        bins=buckets,
        labels=[f"{buckets[i]:.1f}-{buckets[i+1]:.1f}" for i in range(len(buckets) - 1)],
        include_lowest=True,
    )

    calibration = resolved.groupby("prob_bucket", observed=True).agg(
        count=("actual_outcome", "count"),
        actual_frequency=("actual_outcome", "mean"),
        avg_implied_prob=("implied_prob", "mean"),
        avg_volume=("volume", "mean"),
    ).reset_index()

    calibration["forecast_error"] = calibration["actual_frequency"] - calibration["avg_implied_prob"]
    calibration["abs_error"] = calibration["forecast_error"].abs()

    # Overall metrics
    if not resolved.empty:
        brier_score = ((resolved["implied_prob"] - resolved["actual_outcome"]) ** 2).mean()
        calibration.attrs["brier_score"] = brier_score

    calibration.to_csv(config.ANALYSIS_FILE, index=False)
    print(f"Calibration analysis saved to {config.ANALYSIS_FILE}")
    return calibration


def _parse_resolution(resolution):
    """Parse resolution string to binary outcome."""
    if pd.isna(resolution):
        return np.nan
    res = str(resolution).lower().strip()
    if res in ("yes", "1", "true", "p1"):
        return 1.0
    elif res in ("no", "0", "false", "p2"):
        return 0.0
    return np.nan


def identify_mispriced_markets(markets_df, prices_df):
    """Identify potentially mispriced markets based on:
    1. Price vs volume divergence
    2. Large recent price movements (momentum)
    3. Prices far from 0.5 with low volume (illiquid extremes)

    Returns DataFrame of opportunities.
    """
    active = markets_df[markets_df["active"] == True].copy()
    if active.empty:
        print("No active markets to analyze.")
        return pd.DataFrame()

    # Deduplicate to latest snapshot per market
    active = active.sort_values("fetched_at").drop_duplicates("market_id", keep="last")

    opportunities = []

    for _, market in active.iterrows():
        signals = []
        yes_price = float(market.get("outcome_yes_price", 0))
        volume = float(market.get("volume", 0))
        liquidity = float(market.get("liquidity", 0))

        # Signal 1: Low liquidity markets may be mispriced
        if liquidity > 0 and liquidity < 1000 and 0.1 < yes_price < 0.9:
            signals.append("low_liquidity")

        # Signal 2: Check price history for momentum
        if not prices_df.empty:
            history = prices_df[prices_df["market_id"] == market["market_id"]].sort_values("timestamp")
            if len(history) >= 2:
                recent_prices = history["yes_price"].tail(5).values
                if len(recent_prices) >= 2:
                    price_change = recent_prices[-1] - recent_prices[0]
                    if abs(price_change) > 0.1:
                        signals.append(f"momentum_{'+' if price_change > 0 else '-'}{abs(price_change):.2f}")

        # Signal 3: Extreme prices (near 0 or 1) with meaningful volume
        if (yes_price < 0.05 or yes_price > 0.95) and volume > 10000:
            signals.append("extreme_price_high_volume")

        # Signal 4: Mid-range price with very high volume (contested market)
        if 0.35 < yes_price < 0.65 and volume > 50000:
            signals.append("contested_high_volume")

        if signals:
            opportunities.append({
                "market_id": market["market_id"],
                "source": market["source"],
                "title": market["title"],
                "yes_price": yes_price,
                "volume": volume,
                "liquidity": liquidity,
                "signals": "; ".join(signals),
                "analyzed_at": datetime.now(timezone.utc).isoformat(),
            })

    df = pd.DataFrame(opportunities)
    if not df.empty:
        df.to_csv(config.OPPORTUNITIES_FILE, index=False)
        print(f"Found {len(df)} potential opportunities, saved to {config.OPPORTUNITIES_FILE}")
    return df


def detect_slow_reactions(prices_df):
    """Detect markets where price hasn't moved despite new information.

    Returns list of stale markets.
    """
    if prices_df.empty:
        return []

    stale = []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=config.STALE_HOURS)

    for market_id, group in prices_df.groupby("market_id"):
        group = group.sort_values("timestamp")
        if len(group) < 2:
            continue

        recent = group[group["timestamp"] >= cutoff.isoformat()]
        if recent.empty:
            continue

        price_range = recent["yes_price"].max() - recent["yes_price"].min()
        if price_range < 0.01 and float(group.iloc[-1]["volume"]) > 1000:
            stale.append({
                "market_id": market_id,
                "title": group.iloc[-1]["title"],
                "current_price": float(group.iloc[-1]["yes_price"]),
                "hours_stale": config.STALE_HOURS,
                "volume": float(group.iloc[-1]["volume"]),
            })

    return stale


def detect_seasonal_biases(markets_df):
    """Detect if markets show systematic biases by season or region.

    Returns summary dict.
    """
    resolved = markets_df[markets_df["resolved"] == True].copy()
    if resolved.empty:
        return {}

    resolved["actual_outcome"] = resolved["resolution"].apply(_parse_resolution)
    resolved["implied_prob"] = resolved["outcome_yes_price"].astype(float)
    resolved = resolved.dropna(subset=["actual_outcome"])

    if resolved.empty:
        return {}

    # Try to extract month from end_date for seasonal analysis
    resolved["end_month"] = pd.to_datetime(resolved["end_date"], errors="coerce").dt.month

    biases = {}

    # Seasonal bias
    seasonal = resolved.dropna(subset=["end_month"]).groupby("end_month").agg(
        count=("actual_outcome", "count"),
        avg_error=("implied_prob", lambda x: (resolved.loc[x.index, "actual_outcome"] - x).mean()),
    ).to_dict("index")
    if seasonal:
        biases["seasonal"] = seasonal

    # Source bias
    source_bias = resolved.groupby("source").agg(
        count=("actual_outcome", "count"),
        avg_implied=("implied_prob", "mean"),
        avg_actual=("actual_outcome", "mean"),
    ).to_dict("index")
    if source_bias:
        biases["by_source"] = source_bias

    return biases


def generate_summary(markets_df, prices_df, calibration_df, opportunities_df, stale_markets, biases):
    """Generate a concise text summary of insights and opportunities."""
    lines = []
    lines.append("=" * 60)
    lines.append(f"WEATHER PREDICTION MARKET ANALYSIS — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("=" * 60)

    # Market overview
    total = len(markets_df["market_id"].unique()) if not markets_df.empty else 0
    active = len(markets_df[markets_df["active"] == True]["market_id"].unique()) if not markets_df.empty else 0
    resolved = len(markets_df[markets_df["resolved"] == True]["market_id"].unique()) if not markets_df.empty else 0
    lines.append(f"\nMARKETS: {total} total | {active} active | {resolved} resolved")

    if not markets_df.empty:
        for source in markets_df["source"].unique():
            count = len(markets_df[markets_df["source"] == source]["market_id"].unique())
            lines.append(f"  {source}: {count} markets")

    # Price history
    if not prices_df.empty:
        snapshots = len(prices_df)
        lines.append(f"\nPRICE HISTORY: {snapshots} total snapshots")

    # Calibration
    if not calibration_df.empty:
        lines.append("\nCALIBRATION BY PROBABILITY BUCKET:")
        lines.append(f"  {'Bucket':<12} {'Count':>6} {'Implied':>8} {'Actual':>8} {'Error':>8}")
        lines.append(f"  {'-'*44}")
        for _, row in calibration_df.iterrows():
            lines.append(
                f"  {row['prob_bucket']:<12} {row['count']:>6.0f} "
                f"{row['avg_implied_prob']:>8.3f} {row['actual_frequency']:>8.3f} "
                f"{row['forecast_error']:>+8.3f}"
            )
        brier = calibration_df.attrs.get("brier_score")
        if brier is not None:
            lines.append(f"  Brier Score: {brier:.4f}")

    # Opportunities
    if not opportunities_df.empty:
        lines.append(f"\nPOTENTIAL OPPORTUNITIES ({len(opportunities_df)}):")
        for _, opp in opportunities_df.head(10).iterrows():
            lines.append(f"  [{opp['source']}] {opp['title'][:60]}")
            lines.append(f"    Price: {opp['yes_price']:.2f} | Vol: {opp['volume']:,.0f} | Signals: {opp['signals']}")

    # Stale markets
    if stale_markets:
        lines.append(f"\nSLOW/STALE MARKETS ({len(stale_markets)}):")
        for sm in stale_markets[:5]:
            lines.append(f"  {sm['title'][:60]} — price {sm['current_price']:.2f}, stale >{sm['hours_stale']}h")

    # Biases
    if biases:
        lines.append("\nBIAS DETECTION:")
        if "by_source" in biases:
            for source, stats in biases["by_source"].items():
                if stats["count"] >= 5:
                    overunder = "overconfident" if stats["avg_implied"] > stats["avg_actual"] else "underconfident"
                    lines.append(
                        f"  {source}: {overunder} "
                        f"(implied avg {stats['avg_implied']:.3f} vs actual {stats['avg_actual']:.3f}, "
                        f"n={stats['count']})"
                    )

    if total == 0:
        lines.append("\nNo weather markets found in this session.")
        lines.append("This could be due to network restrictions or no current weather markets on these platforms.")
        lines.append("The system is configured and ready — data will accumulate across sessions.")

    lines.append("\n" + "=" * 60)

    summary = "\n".join(lines)

    with open(config.SUMMARY_FILE, "w") as f:
        f.write(summary)
    print(f"\nSummary saved to {config.SUMMARY_FILE}")
    return summary


if __name__ == "__main__":
    try:
        markets_df = pd.read_csv(config.MARKETS_FILE)
    except FileNotFoundError:
        markets_df = pd.DataFrame()

    try:
        prices_df = pd.read_csv(config.PRICES_FILE)
    except FileNotFoundError:
        prices_df = pd.DataFrame()

    calibration_df = compute_calibration(markets_df)
    if calibration_df.empty:
        calibration_df = pd.DataFrame()

    opportunities_df = identify_mispriced_markets(markets_df, prices_df)
    stale = detect_slow_reactions(prices_df)
    biases = detect_seasonal_biases(markets_df)

    summary = generate_summary(markets_df, prices_df, calibration_df, opportunities_df, stale, biases)
    print(summary)
