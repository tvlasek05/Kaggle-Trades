"""
Analysis engine for weather prediction markets.

Computes:
- Implied probability vs actual outcome
- Forecast error
- Calibration by probability bucket
- Identifies mispriced markets, slow reactions, seasonal/regional biases
"""

import json
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
ANALYSIS_DIR = os.path.join(os.path.dirname(__file__), "analysis")
MARKET_PRICES_FILE = os.path.join(DATA_DIR, "market_prices.csv")
ACTUALS_FILE = os.path.join(DATA_DIR, "weather_actuals.csv")


def load_prices():
    if os.path.exists(MARKET_PRICES_FILE):
        return pd.read_csv(MARKET_PRICES_FILE)
    return pd.DataFrame()


def load_actuals():
    if os.path.exists(ACTUALS_FILE):
        return pd.read_csv(ACTUALS_FILE)
    return pd.DataFrame()


# --- Calibration Analysis ---

def compute_calibration(prices_df):
    """
    Compute calibration: for resolved markets, group by probability bucket
    and compare implied probability to actual resolution rate.
    """
    if prices_df.empty or "resolved" not in prices_df.columns:
        return None
    resolved = prices_df[prices_df["resolved"] == True].copy()
    if resolved.empty:
        return None

    # For 'Yes' outcomes, the price IS the implied probability
    yes_prices = resolved[resolved["outcome"].str.lower() == "yes"].copy()
    if yes_prices.empty:
        return None

    # Determine actual outcome: resolution == "yes" means event happened
    yes_prices["actual"] = yes_prices["resolution"].apply(
        lambda r: 1.0 if str(r).lower() in ("yes", "1", "true", "y") else 0.0
    )
    yes_prices["implied_prob"] = yes_prices["price"].clip(0, 1)

    # Bucket by implied probability (0-10%, 10-20%, ..., 90-100%)
    bins = np.arange(0, 1.1, 0.1)
    labels = [f"{int(b*100)}-{int((b+0.1)*100)}%" for b in bins[:-1]]
    yes_prices["bucket"] = pd.cut(yes_prices["implied_prob"], bins=bins, labels=labels, include_lowest=True)

    calibration = yes_prices.groupby("bucket", observed=True).agg(
        mean_implied=("implied_prob", "mean"),
        actual_rate=("actual", "mean"),
        count=("actual", "count"),
    ).reset_index()

    calibration["calibration_error"] = calibration["mean_implied"] - calibration["actual_rate"]
    return calibration


# --- Forecast Error ---

def compute_forecast_errors(prices_df):
    """
    For resolved markets, compute Brier score and log loss per market.
    """
    if prices_df.empty or "resolved" not in prices_df.columns:
        return None
    resolved = prices_df[prices_df["resolved"] == True].copy()
    if resolved.empty:
        return None

    yes_prices = resolved[resolved["outcome"].str.lower() == "yes"].copy()
    if yes_prices.empty:
        return None

    yes_prices["actual"] = yes_prices["resolution"].apply(
        lambda r: 1.0 if str(r).lower() in ("yes", "1", "true", "y") else 0.0
    )
    yes_prices["implied_prob"] = yes_prices["price"].clip(0.001, 0.999)

    # Brier score per market (lower is better)
    yes_prices["brier_score"] = (yes_prices["implied_prob"] - yes_prices["actual"]) ** 2

    # Log loss per market (lower is better)
    yes_prices["log_loss"] = -(
        yes_prices["actual"] * np.log(yes_prices["implied_prob"])
        + (1 - yes_prices["actual"]) * np.log(1 - yes_prices["implied_prob"])
    )

    # Aggregate by market
    errors = yes_prices.groupby("market_id").agg(
        question=("question", "first"),
        source=("source", "first"),
        last_price=("implied_prob", "last"),
        actual=("actual", "first"),
        brier_score=("brier_score", "mean"),
        log_loss=("log_loss", "mean"),
        n_snapshots=("implied_prob", "count"),
    ).reset_index()

    errors = errors.sort_values("brier_score", ascending=False)
    return errors


# --- Mispricing Detection ---

def detect_mispriced_markets(prices_df):
    """
    Identify potentially mispriced markets based on:
    1. Yes + No prices not summing to ~1.0 (arbitrage opportunity)
    2. Large price movements (may indicate slow reaction)
    3. Extreme prices with low volume (thin markets)
    """
    findings = []
    if prices_df.empty:
        return findings
    latest = prices_df.sort_values("timestamp").groupby(
        ["market_id", "outcome"]
    ).last().reset_index()

    # Check for arbitrage: Yes + No should sum to ~1.0
    for market_id in latest["market_id"].unique():
        mkt = latest[latest["market_id"] == market_id]
        yes_row = mkt[mkt["outcome"].str.lower() == "yes"]
        no_row = mkt[mkt["outcome"].str.lower() == "no"]

        if not yes_row.empty and not no_row.empty:
            yes_p = float(yes_row["price"].iloc[0])
            no_p = float(no_row["price"].iloc[0])
            total = yes_p + no_p
            if abs(total - 1.0) > 0.05:  # >5% deviation
                findings.append({
                    "type": "arbitrage",
                    "market_id": market_id,
                    "question": yes_row["question"].iloc[0],
                    "source": yes_row["source"].iloc[0],
                    "yes_price": yes_p,
                    "no_price": no_p,
                    "total": total,
                    "spread": abs(total - 1.0),
                    "detail": f"Yes+No = {total:.3f} (deviation: {abs(total-1.0):.3f})",
                })

    # Check for large recent price movements (slow market reactions)
    if len(prices_df) > 1:
        prices_df_sorted = prices_df.sort_values("timestamp")
        for market_id in prices_df_sorted["market_id"].unique():
            mkt = prices_df_sorted[
                (prices_df_sorted["market_id"] == market_id)
                & (prices_df_sorted["outcome"].str.lower() == "yes")
            ]
            if len(mkt) >= 2:
                recent = mkt.tail(5)
                price_range = recent["price"].max() - recent["price"].min()
                if price_range > 0.15:  # >15% swing in recent snapshots
                    findings.append({
                        "type": "volatility",
                        "market_id": market_id,
                        "question": mkt["question"].iloc[0],
                        "source": mkt["source"].iloc[0],
                        "price_range": price_range,
                        "current_price": float(mkt["price"].iloc[-1]),
                        "detail": f"Price range {price_range:.3f} in recent snapshots",
                    })

    # Check for thin markets at extreme prices
    for market_id in latest["market_id"].unique():
        mkt = latest[latest["market_id"] == market_id]
        yes_row = mkt[mkt["outcome"].str.lower() == "yes"]
        if not yes_row.empty:
            price = float(yes_row["price"].iloc[0])
            vol = float(yes_row["volume"].iloc[0]) if "volume" in yes_row.columns else 0
            if (price > 0.85 or price < 0.15) and vol < 1000:
                findings.append({
                    "type": "thin_extreme",
                    "market_id": market_id,
                    "question": yes_row["question"].iloc[0],
                    "source": yes_row["source"].iloc[0],
                    "price": price,
                    "volume": vol,
                    "detail": f"Extreme price {price:.3f} with low volume {vol}",
                })

    return findings


# --- Seasonal/Regional Bias ---

def detect_biases(prices_df, actuals_df):
    """
    Identify systematic biases by region or season in resolved markets.
    """
    biases = []
    if prices_df.empty or "resolved" not in prices_df.columns:
        return biases
    resolved = prices_df[prices_df["resolved"] == True].copy()
    if resolved.empty:
        return biases

    yes_prices = resolved[resolved["outcome"].str.lower() == "yes"].copy()
    if yes_prices.empty:
        return biases

    yes_prices["actual"] = yes_prices["resolution"].apply(
        lambda r: 1.0 if str(r).lower() in ("yes", "1", "true", "y") else 0.0
    )
    yes_prices["implied_prob"] = yes_prices["price"].clip(0, 1)
    yes_prices["error"] = yes_prices["implied_prob"] - yes_prices["actual"]

    # Regional bias: check if certain cities are systematically over/under predicted
    city_keywords = {
        "new_york": ["new york", "nyc", "ny"],
        "los_angeles": ["los angeles", "la"],
        "chicago": ["chicago"],
        "miami": ["miami"],
        "houston": ["houston"],
    }

    for city, keywords in city_keywords.items():
        mask = yes_prices["question"].str.lower().apply(
            lambda q: any(kw in q for kw in keywords)
        )
        city_markets = yes_prices[mask]
        if len(city_markets) >= 3:
            mean_error = city_markets["error"].mean()
            if abs(mean_error) > 0.05:
                direction = "overestimated" if mean_error > 0 else "underestimated"
                biases.append({
                    "type": "regional",
                    "region": city,
                    "mean_error": mean_error,
                    "n_markets": len(city_markets),
                    "detail": f"{city}: probability {direction} by {abs(mean_error):.1%} (n={len(city_markets)})",
                })

    # Source bias: compare Polymarket vs Kalshi calibration
    for source in yes_prices["source"].unique():
        src_markets = yes_prices[yes_prices["source"] == source]
        if len(src_markets) >= 5:
            mean_error = src_markets["error"].mean()
            if abs(mean_error) > 0.03:
                biases.append({
                    "type": "source",
                    "source": source,
                    "mean_error": mean_error,
                    "n_markets": len(src_markets),
                    "detail": f"{source}: mean error {mean_error:+.3f} (n={len(src_markets)})",
                })

    return biases


# --- Summary Report ---

def generate_analysis_report(prices_df, actuals_df):
    """Generate a complete analysis report."""
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_summary": {},
        "calibration": None,
        "forecast_errors": None,
        "mispriced_markets": [],
        "biases": [],
        "trading_opportunities": [],
    }

    # Data summary
    report["data_summary"] = {
        "total_price_snapshots": len(prices_df),
        "unique_markets": prices_df["market_id"].nunique() if not prices_df.empty else 0,
        "sources": prices_df["source"].unique().tolist() if not prices_df.empty else [],
        "resolved_markets": int(prices_df[prices_df["resolved"] == True]["market_id"].nunique()) if not prices_df.empty else 0,
        "weather_records": len(actuals_df),
        "cities_tracked": actuals_df["city"].nunique() if not actuals_df.empty else 0,
    }

    # Calibration
    cal = compute_calibration(prices_df)
    if cal is not None:
        report["calibration"] = cal.to_dict(orient="records")

    # Forecast errors
    errors = compute_forecast_errors(prices_df)
    if errors is not None:
        report["forecast_errors"] = errors.head(20).to_dict(orient="records")

    # Mispricing
    mispriced = detect_mispriced_markets(prices_df)
    report["mispriced_markets"] = mispriced

    # Biases
    biases = detect_biases(prices_df, actuals_df)
    report["biases"] = biases

    # Trading opportunities: combine mispricing signals
    opportunities = []
    for m in mispriced:
        if m["type"] == "arbitrage" and m["spread"] > 0.08:
            opportunities.append({
                "signal": "arbitrage",
                "market": m["question"][:100],
                "detail": m["detail"],
                "strength": "strong" if m["spread"] > 0.15 else "moderate",
            })
        elif m["type"] == "thin_extreme":
            opportunities.append({
                "signal": "thin_market",
                "market": m["question"][:100],
                "detail": m["detail"],
                "strength": "speculative",
            })

    report["trading_opportunities"] = opportunities
    return report


def save_report(report):
    """Save analysis report to JSON."""
    os.makedirs(ANALYSIS_DIR, exist_ok=True)
    filepath = os.path.join(ANALYSIS_DIR, "latest_report.json")
    with open(filepath, "w") as f:
        json.dump(report, f, indent=2, default=str)
    return filepath


def format_summary(report):
    """Format a concise text summary of the analysis."""
    lines = []
    lines.append("=" * 60)
    lines.append("WEATHER PREDICTION MARKET ANALYSIS")
    lines.append(f"Generated: {report['generated_at']}")
    lines.append("=" * 60)

    ds = report["data_summary"]
    lines.append(f"\n--- Data Summary ---")
    lines.append(f"Price snapshots: {ds['total_price_snapshots']}")
    lines.append(f"Unique markets:  {ds['unique_markets']}")
    lines.append(f"Sources:         {', '.join(ds['sources'])}")
    lines.append(f"Resolved:        {ds['resolved_markets']}")
    lines.append(f"Weather records: {ds['weather_records']}")
    lines.append(f"Cities tracked:  {ds['cities_tracked']}")

    # Calibration
    cal = report.get("calibration")
    if cal:
        lines.append(f"\n--- Calibration by Probability Bucket ---")
        for row in cal:
            lines.append(
                f"  {row['bucket']:>10s}: implied={row['mean_implied']:.3f}  "
                f"actual={row['actual_rate']:.3f}  "
                f"error={row['calibration_error']:+.3f}  "
                f"(n={row['count']})"
            )

    # Top forecast errors
    errors = report.get("forecast_errors")
    if errors:
        lines.append(f"\n--- Worst Forecast Errors (by Brier Score) ---")
        for row in errors[:5]:
            lines.append(
                f"  [{row['source']}] {row['question'][:60]}"
                f"\n    Price={row['last_price']:.3f} Actual={row['actual']:.0f} "
                f"Brier={row['brier_score']:.4f}"
            )

    # Mispriced markets
    mispriced = report.get("mispriced_markets", [])
    if mispriced:
        lines.append(f"\n--- Mispriced Markets ({len(mispriced)} found) ---")
        for m in mispriced[:10]:
            lines.append(f"  [{m['type']}] {m['question'][:60]}")
            lines.append(f"    {m['detail']}")

    # Biases
    biases = report.get("biases", [])
    if biases:
        lines.append(f"\n--- Detected Biases ---")
        for b in biases:
            lines.append(f"  [{b['type']}] {b['detail']}")

    # Trading opportunities
    opps = report.get("trading_opportunities", [])
    if opps:
        lines.append(f"\n--- Trading Opportunities ({len(opps)}) ---")
        for o in opps:
            lines.append(f"  [{o['strength'].upper()}] {o['signal']}: {o['market'][:60]}")
            lines.append(f"    {o['detail']}")
    else:
        lines.append(f"\n--- No clear trading opportunities identified ---")

    lines.append("\n" + "=" * 60)
    return "\n".join(lines)


if __name__ == "__main__":
    prices = load_prices()
    actuals = load_actuals()
    report = generate_analysis_report(prices, actuals)
    save_report(report)
    print(format_summary(report))
