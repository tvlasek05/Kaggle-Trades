"""Analysis engine: calibration, mispricing detection, bias identification."""

import os
from datetime import datetime, timezone

import pandas as pd

import config


def compute_calibration(outcomes_df):
    """Compute calibration by probability bucket.

    For each bucket (e.g., 0.0-0.1, 0.1-0.2, ...):
    - Count how many markets had implied probabilities in that range
    - Count how many actually resolved YES
    - Compare implied probability vs actual frequency
    """
    if outcomes_df.empty or "implied_prob" not in outcomes_df.columns:
        return pd.DataFrame()

    df = outcomes_df.copy()
    df["implied_prob"] = pd.to_numeric(df["implied_prob"], errors="coerce")

    # Determine actual outcome (1 = YES, 0 = NO)
    df["actual_yes"] = df["market_outcome"].apply(
        lambda x: 1 if str(x).lower() in ("yes", "1", "true", "1.0") else 0
    )

    buckets = config.CALIBRATION_BUCKETS
    rows = []

    for i in range(len(buckets) - 1):
        lo, hi = buckets[i], buckets[i + 1]
        label = f"{lo:.1f}-{hi:.1f}"
        mask = (df["implied_prob"] >= lo) & (df["implied_prob"] < hi)
        bucket_df = df[mask]

        if len(bucket_df) == 0:
            rows.append({
                "bucket": label,
                "bucket_midpoint": (lo + hi) / 2,
                "count": 0,
                "actual_rate": None,
                "avg_implied_prob": None,
                "calibration_error": None,
            })
        else:
            actual_rate = bucket_df["actual_yes"].mean()
            avg_implied = bucket_df["implied_prob"].mean()
            rows.append({
                "bucket": label,
                "bucket_midpoint": (lo + hi) / 2,
                "count": len(bucket_df),
                "actual_rate": round(actual_rate, 4),
                "avg_implied_prob": round(avg_implied, 4),
                "calibration_error": round(actual_rate - avg_implied, 4),
            })

    cal_df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(config.CALIBRATION_FILE), exist_ok=True)
    cal_df.to_csv(config.CALIBRATION_FILE, index=False)
    return cal_df


def compute_forecast_errors(outcomes_df):
    """Compute forecast errors for resolved markets with weather verification."""
    if outcomes_df.empty:
        return pd.DataFrame()

    df = outcomes_df.copy()
    df["implied_prob"] = pd.to_numeric(df["implied_prob"], errors="coerce")
    df["actual_yes"] = df["market_outcome"].apply(
        lambda x: 1.0 if str(x).lower() in ("yes", "1", "true", "1.0") else 0.0
    )

    # Forecast error = implied_prob - actual_outcome
    df["forecast_error"] = df["implied_prob"] - df["actual_yes"]
    df["abs_error"] = df["forecast_error"].abs()
    df["brier_score"] = df["forecast_error"] ** 2

    return df[["source", "market_id", "question", "implied_prob",
               "actual_yes", "forecast_error", "abs_error", "brier_score"]]


def identify_mispriced_markets(markets_df, price_history_df, forecasts=None):
    """Identify potentially mispriced markets.

    Signals:
    1. Large price moves (momentum/reversal)
    2. Prices far from 0.5 with low volume (illiquid extremes)
    3. Markets where weather forecast strongly disagrees with implied prob
    """
    insights = []

    if markets_df.empty:
        return insights

    active = markets_df[markets_df["resolved"] == False].copy()
    active["last_price"] = pd.to_numeric(active["last_price"], errors="coerce")
    active["volume"] = pd.to_numeric(active["volume"], errors="coerce")

    # 1. Extreme prices with low volume (potential mispricings)
    for _, m in active.iterrows():
        price = m.get("last_price")
        vol = m.get("volume", 0)
        if price is None or pd.isna(price):
            continue

        # Very high or very low implied probability with thin volume
        if vol is not None and not pd.isna(vol) and vol < 10000:
            if price > 0.85 or price < 0.15:
                insights.append({
                    "type": "illiquid_extreme",
                    "source": m["source"],
                    "market_id": m["market_id"],
                    "question": m["question"],
                    "price": price,
                    "volume": vol,
                    "signal": f"Price {price:.2f} with only ${vol:.0f} volume - potential mispricing",
                })

    # 2. Detect large recent price movements from price history
    if not price_history_df.empty:
        ph = price_history_df.copy()
        ph["price"] = pd.to_numeric(ph["price"], errors="coerce")
        ph["timestamp"] = pd.to_datetime(ph["timestamp"], errors="coerce")

        for (src, mid), group in ph.groupby(["source", "market_id"]):
            if len(group) < 2:
                continue
            group = group.sort_values("timestamp")
            prices = group["price"].dropna()
            if len(prices) >= 2:
                first = prices.iloc[0]
                last = prices.iloc[-1]
                change = last - first
                if abs(change) > 0.15:
                    direction = "up" if change > 0 else "down"
                    insights.append({
                        "type": "large_move",
                        "source": src,
                        "market_id": mid,
                        "question": "",
                        "price": last,
                        "volume": None,
                        "signal": f"Price moved {direction} by {abs(change):.2f} ({first:.2f} -> {last:.2f})",
                    })

    return insights


def detect_slow_reactions(price_history_df):
    """Detect markets that are slow to react to information.

    Look for gradual price drift in one direction (trending markets).
    """
    insights = []

    if price_history_df.empty:
        return insights

    ph = price_history_df.copy()
    ph["price"] = pd.to_numeric(ph["price"], errors="coerce")
    ph["timestamp"] = pd.to_datetime(ph["timestamp"], errors="coerce")

    for (src, mid), group in ph.groupby(["source", "market_id"]):
        group = group.sort_values("timestamp")
        prices = group["price"].dropna().values

        if len(prices) < 3:
            continue

        # Check for monotonic drift (all moves in same direction)
        diffs = [prices[i + 1] - prices[i] for i in range(len(prices) - 1)]
        positive = sum(1 for d in diffs if d > 0.01)
        negative = sum(1 for d in diffs if d < -0.01)
        total_nonzero = positive + negative

        if total_nonzero >= 3:
            ratio = max(positive, negative) / total_nonzero
            if ratio > 0.8:
                direction = "upward" if positive > negative else "downward"
                total_move = prices[-1] - prices[0]
                insights.append({
                    "type": "slow_drift",
                    "source": src,
                    "market_id": mid,
                    "signal": f"Consistent {direction} drift: {total_move:+.2f} over {len(prices)} snapshots",
                })

    return insights


def detect_seasonal_regional_biases(outcomes_df, markets_df):
    """Detect seasonal or regional biases in prediction accuracy."""
    biases = []

    if outcomes_df.empty:
        return biases

    df = outcomes_df.copy()
    df["implied_prob"] = pd.to_numeric(df["implied_prob"], errors="coerce")
    df["actual_yes"] = df["market_outcome"].apply(
        lambda x: 1.0 if str(x).lower() in ("yes", "1", "true", "1.0") else 0.0
    )
    df["error"] = df["implied_prob"] - df["actual_yes"]

    # Regional bias: group by city
    if "city" in df.columns:
        for city, group in df.groupby("city"):
            if len(group) >= 3:
                mean_error = group["error"].mean()
                if abs(mean_error) > 0.1:
                    direction = "overestimates" if mean_error > 0 else "underestimates"
                    biases.append({
                        "type": "regional_bias",
                        "detail": f"{city.title()}: Market {direction} by {abs(mean_error):.2f} on avg ({len(group)} markets)",
                    })

    # Seasonal bias: group by month of target date
    if "target_date" in df.columns:
        df["month"] = pd.to_datetime(df["target_date"], errors="coerce").dt.month
        for month, group in df.groupby("month"):
            if len(group) >= 3 and pd.notna(month):
                mean_error = group["error"].mean()
                if abs(mean_error) > 0.1:
                    direction = "overestimates" if mean_error > 0 else "underestimates"
                    month_name = datetime(2000, int(month), 1).strftime("%B")
                    biases.append({
                        "type": "seasonal_bias",
                        "detail": f"{month_name}: Market {direction} by {abs(mean_error):.2f} on avg ({len(group)} markets)",
                    })

    # Source bias
    for source, group in df.groupby("source"):
        if len(group) >= 3:
            mean_error = group["error"].mean()
            brier = (group["error"] ** 2).mean()
            biases.append({
                "type": "source_bias",
                "detail": f"{source}: Mean error {mean_error:+.3f}, Brier score {brier:.3f} ({len(group)} markets)",
            })

    return biases


def run_analysis(markets_df, price_history_df, outcomes_df, forecasts=None):
    """Run full analysis suite. Returns dict of results."""
    results = {
        "calibration": pd.DataFrame(),
        "forecast_errors": pd.DataFrame(),
        "mispriced": [],
        "slow_reactions": [],
        "biases": [],
    }

    # Calibration
    if not outcomes_df.empty:
        results["calibration"] = compute_calibration(outcomes_df)
        results["forecast_errors"] = compute_forecast_errors(outcomes_df)
        results["biases"] = detect_seasonal_regional_biases(outcomes_df, markets_df)

    # Mispricing detection
    results["mispriced"] = identify_mispriced_markets(
        markets_df, price_history_df, forecasts
    )

    # Slow reaction detection
    results["slow_reactions"] = detect_slow_reactions(price_history_df)

    # Save analysis results
    _save_analysis_results(results)

    return results


def _save_analysis_results(results):
    """Persist analysis results to files."""
    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    # Save detailed analysis
    rows = []
    for item in results.get("mispriced", []):
        rows.append({"category": "mispriced", **item})
    for item in results.get("slow_reactions", []):
        rows.append({"category": "slow_reaction", **item})
    for item in results.get("biases", []):
        rows.append({"category": "bias", **item})

    if rows:
        pd.DataFrame(rows).to_csv(config.ANALYSIS_FILE, index=False)

    if not results["forecast_errors"].empty:
        results["forecast_errors"].to_csv(
            os.path.join(config.RESULTS_DIR, "forecast_errors.csv"), index=False
        )
