"""Analysis engine: calibration, mispricing, bias detection."""

import json
import logging
import re
from datetime import datetime, timezone

import numpy as np
import pandas as pd

import config

logger = logging.getLogger(__name__)


def load_snapshots():
    """Load market snapshot history."""
    try:
        return pd.read_csv(config.MARKET_SNAPSHOTS_FILE)
    except FileNotFoundError:
        return pd.DataFrame()


def load_resolved():
    """Load resolved market records."""
    try:
        return pd.read_csv(config.RESOLVED_MARKETS_FILE)
    except FileNotFoundError:
        return pd.DataFrame()


def load_weather_actuals():
    """Load weather actuals data."""
    try:
        return pd.read_csv(config.WEATHER_ACTUALS_FILE)
    except FileNotFoundError:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Resolved market matching
# ---------------------------------------------------------------------------

def extract_date_from_ticker(ticker):
    """Extract date from Kalshi ticker like KXHIGHNY-26MAR22."""
    match = re.search(r"-(\d{2})([A-Z]{3})(\d{2})", str(ticker))
    if not match:
        return None
    day, mon_str, yr = match.groups()
    months = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
              "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
    month = months.get(mon_str)
    if not month:
        return None
    year = 2000 + int(yr)
    try:
        return f"{year}-{month:02d}-{int(day):02d}"
    except ValueError:
        return None


def extract_strike_from_market(market_row):
    """Extract temperature strike from market data."""
    strike = market_row.get("floor_strike")
    if strike is not None and str(strike) != "" and str(strike) != "None":
        try:
            return float(strike)
        except (ValueError, TypeError):
            pass
    # Try parsing from title
    title = str(market_row.get("title", ""))
    match = re.search(r"(\d+)\s*(?:degrees|°|F)", title, re.IGNORECASE)
    if match:
        return float(match.group(1))
    return None


def match_resolved_markets(snapshots_df, weather_df):
    """Match resolved Kalshi temperature markets with actual weather outcomes."""
    if snapshots_df.empty or weather_df.empty:
        return pd.DataFrame()

    resolved = snapshots_df[
        (snapshots_df["source"] == "kalshi") &
        (snapshots_df["result"].isin(["yes", "no"]))
    ].copy()

    if resolved.empty:
        return pd.DataFrame()

    # Deduplicate: keep latest snapshot per market
    resolved = resolved.sort_values("snapshot_time").drop_duplicates("market_id", keep="last")

    rows = []
    for _, mkt in resolved.iterrows():
        event_id = str(mkt.get("event_id", ""))
        series = str(mkt.get("series", ""))
        city = config.SERIES_CITY_MAP.get(series)
        date = extract_date_from_ticker(event_id)
        strike = extract_strike_from_market(mkt)

        if not city or not date:
            continue

        # Find matching weather actual
        wx = weather_df[(weather_df["city"] == city) & (weather_df["date"] == date)]
        if wx.empty:
            continue

        actual_high = wx.iloc[0].get("temp_max_f")
        if actual_high is None or pd.isna(actual_high):
            continue

        actual_outcome = None
        if strike is not None and "HIGH" in series:
            actual_outcome = "yes" if actual_high >= strike else "no"

        rows.append({
            "market_id": mkt["market_id"],
            "event_id": event_id,
            "series": series,
            "city": city,
            "date": date,
            "strike": strike,
            "implied_prob": float(mkt["yes_price"]),
            "market_result": mkt["result"],
            "actual_high_f": float(actual_high),
            "actual_outcome": actual_outcome,
            "correct": mkt["result"] == actual_outcome if actual_outcome else None,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Calibration analysis
# ---------------------------------------------------------------------------

def compute_calibration(resolved_df):
    """Compute calibration by probability bucket."""
    if resolved_df.empty or "implied_prob" not in resolved_df.columns:
        return []

    if "actual_outcome" not in resolved_df.columns:
        return []
    df = resolved_df.dropna(subset=["actual_outcome"])
    if df.empty:
        return {}

    df = df.copy()
    df["outcome_binary"] = (df["actual_outcome"] == "yes").astype(int)
    df["prob_bucket"] = pd.cut(
        df["implied_prob"],
        bins=config.CALIBRATION_BUCKETS,
        include_lowest=True,
    )

    cal = df.groupby("prob_bucket", observed=True).agg(
        count=("outcome_binary", "count"),
        mean_implied_prob=("implied_prob", "mean"),
        actual_frequency=("outcome_binary", "mean"),
    ).reset_index()

    cal["calibration_error"] = cal["actual_frequency"] - cal["mean_implied_prob"]
    cal["prob_bucket"] = cal["prob_bucket"].astype(str)

    return cal.to_dict(orient="records")


def compute_forecast_errors(resolved_df):
    """Compute Brier score and log-loss style errors."""
    if resolved_df.empty or "actual_outcome" not in resolved_df.columns:
        return {}
    df = resolved_df.dropna(subset=["actual_outcome"]).copy()
    if df.empty:
        return {}

    df["outcome_binary"] = (df["actual_outcome"] == "yes").astype(int)
    df["brier"] = (df["implied_prob"] - df["outcome_binary"]) ** 2

    eps = 1e-8
    df["log_loss"] = -(
        df["outcome_binary"] * np.log(df["implied_prob"].clip(eps, 1 - eps)) +
        (1 - df["outcome_binary"]) * np.log((1 - df["implied_prob"]).clip(eps, 1 - eps))
    )

    return {
        "n_markets": len(df),
        "mean_brier_score": round(float(df["brier"].mean()), 4),
        "mean_log_loss": round(float(df["log_loss"].mean()), 4),
        "brier_by_series": df.groupby("series")["brier"].mean().round(4).to_dict(),
    }


# ---------------------------------------------------------------------------
# Mispricing and bias detection
# ---------------------------------------------------------------------------

def detect_mispriced_markets(snapshots_df, weather_df):
    """
    Identify potentially mispriced markets by comparing implied probability
    to a simple weather-model-based estimate.
    """
    if snapshots_df.empty or weather_df.empty:
        return []

    active = snapshots_df[
        (snapshots_df["source"] == "kalshi") &
        (snapshots_df["status"] == "active")
    ].copy()

    if active.empty:
        return []

    active = active.sort_values("snapshot_time").drop_duplicates("market_id", keep="last")
    opportunities = []

    for _, mkt in active.iterrows():
        series = str(mkt.get("series", ""))
        if "HIGH" not in series:
            continue

        city = config.SERIES_CITY_MAP.get(series)
        event_id = str(mkt.get("event_id", ""))
        date = extract_date_from_ticker(event_id)
        strike = extract_strike_from_market(mkt)

        if not city or not date or strike is None:
            continue

        # Check if we have a forecast for this date
        wx = weather_df[(weather_df["city"] == city) & (weather_df["date"] == date)]
        if wx.empty:
            continue

        forecast_high = wx.iloc[0].get("temp_max_f")
        if forecast_high is None or pd.isna(forecast_high):
            continue

        # Simple model: estimate probability based on distance from strike
        # Using a rough normal approximation with ~5°F typical forecast error
        forecast_error_std = 5.0
        z = (float(forecast_high) - float(strike)) / forecast_error_std
        # Approximate CDF using a sigmoid
        model_prob = 1.0 / (1.0 + np.exp(-1.7 * z))
        model_prob = round(float(model_prob), 4)

        implied = float(mkt["yes_price"])
        edge = model_prob - implied

        if abs(edge) > 0.08:  # 8%+ edge threshold
            opportunities.append({
                "market_id": mkt["market_id"],
                "title": mkt.get("title", ""),
                "city": city,
                "date": date,
                "strike_f": strike,
                "forecast_high_f": float(forecast_high),
                "implied_prob": implied,
                "model_prob": model_prob,
                "edge": round(edge, 4),
                "signal": "BUY YES" if edge > 0 else "BUY NO",
                "volume": int(mkt.get("volume", 0) or 0),
            })

    opportunities.sort(key=lambda x: abs(x["edge"]), reverse=True)
    return opportunities


def detect_slow_reactions(snapshots_df):
    """Detect markets slow to react by measuring price stickiness over time."""
    if snapshots_df.empty:
        return []

    df = snapshots_df[snapshots_df["source"] == "kalshi"].copy()
    if df.empty or "snapshot_time" not in df.columns:
        return []

    df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], errors="coerce")
    df = df.dropna(subset=["snapshot_time"])

    # Need at least 2 snapshots per market
    market_counts = df.groupby("market_id").size()
    multi_snap = market_counts[market_counts >= 2].index
    df = df[df["market_id"].isin(multi_snap)]

    if df.empty:
        return []

    slow = []
    for mid, group in df.groupby("market_id"):
        group = group.sort_values("snapshot_time")
        prices = group["yes_price"].values
        times = group["snapshot_time"].values

        if len(prices) < 2:
            continue

        # Compute max price change
        price_range = float(np.max(prices) - np.min(prices))
        # Time span
        time_span = (times[-1] - times[0]) / np.timedelta64(1, "h")

        if time_span > 0 and price_range > 0.15:
            slow.append({
                "market_id": str(mid),
                "title": group.iloc[-1].get("title", ""),
                "price_range": round(price_range, 4),
                "hours_span": round(float(time_span), 1),
                "price_velocity": round(price_range / float(time_span), 4),
                "latest_price": round(float(prices[-1]), 4),
                "snapshots": len(prices),
            })

    slow.sort(key=lambda x: x["price_velocity"], reverse=True)
    return slow[:20]


def detect_regional_biases(resolved_df):
    """Check if certain cities/regions show systematic forecast errors."""
    if resolved_df.empty or "city" not in resolved_df.columns:
        return {}

    df = resolved_df.dropna(subset=["actual_outcome"]).copy()
    if df.empty:
        return {}

    df["outcome_binary"] = (df["actual_outcome"] == "yes").astype(int)
    df["brier"] = (df["implied_prob"] - df["outcome_binary"]) ** 2
    df["bias"] = df["implied_prob"] - df["outcome_binary"]

    result = {}
    for city, grp in df.groupby("city"):
        result[city] = {
            "n_markets": len(grp),
            "mean_brier": round(float(grp["brier"].mean()), 4),
            "mean_bias": round(float(grp["bias"].mean()), 4),
            "bias_direction": "overestimates YES" if grp["bias"].mean() > 0.02
                             else "overestimates NO" if grp["bias"].mean() < -0.02
                             else "well-calibrated",
        }

    return result


def detect_seasonal_biases(resolved_df):
    """Check if calibration varies by month/season."""
    if resolved_df.empty or "date" not in resolved_df.columns:
        return {}

    df = resolved_df.dropna(subset=["actual_outcome", "date"]).copy()
    if df.empty:
        return {}

    df["month"] = pd.to_datetime(df["date"], errors="coerce").dt.month
    df = df.dropna(subset=["month"])
    df["outcome_binary"] = (df["actual_outcome"] == "yes").astype(int)
    df["brier"] = (df["implied_prob"] - df["outcome_binary"]) ** 2
    df["bias"] = df["implied_prob"] - df["outcome_binary"]

    month_names = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
                   7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}

    result = {}
    for month, grp in df.groupby("month"):
        name = month_names.get(int(month), str(month))
        result[name] = {
            "n_markets": len(grp),
            "mean_brier": round(float(grp["brier"].mean()), 4),
            "mean_bias": round(float(grp["bias"].mean()), 4),
        }

    return result


# ---------------------------------------------------------------------------
# Full analysis
# ---------------------------------------------------------------------------

def run_full_analysis():
    """Run all analyses and return results dict."""
    snapshots = load_snapshots()
    weather = load_weather_actuals()
    resolved = load_resolved()

    # If no pre-existing resolved file, try to match from snapshots
    if resolved.empty and not snapshots.empty and not weather.empty:
        resolved = match_resolved_markets(snapshots, weather)
        if not resolved.empty:
            resolved.to_csv(config.RESOLVED_MARKETS_FILE, index=False)
            logger.info("Saved %d resolved market records", len(resolved))

    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data_summary": {
            "total_snapshots": len(snapshots),
            "unique_markets": int(snapshots["market_id"].nunique()) if not snapshots.empty else 0,
            "resolved_markets": len(resolved),
            "weather_rows": len(weather),
            "sources": snapshots["source"].value_counts().to_dict() if not snapshots.empty else {},
        },
        "calibration": compute_calibration(resolved),
        "forecast_errors": compute_forecast_errors(resolved),
        "mispriced_markets": detect_mispriced_markets(snapshots, weather),
        "slow_reactions": detect_slow_reactions(snapshots),
        "regional_biases": detect_regional_biases(resolved),
        "seasonal_biases": detect_seasonal_biases(resolved),
    }

    return results
