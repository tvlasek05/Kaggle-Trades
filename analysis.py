"""Analysis module: calibration, mispricing, bias detection."""

import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timezone

import config

logger = logging.getLogger(__name__)


def compute_calibration(markets_df: pd.DataFrame, outcomes_df: pd.DataFrame) -> dict:
    """
    Compute calibration by probability bucket.

    For resolved markets with known outcomes, compare implied probability
    to actual resolution rate.
    """
    resolved = markets_df[markets_df["resolved"] == True].copy()
    if resolved.empty:
        return {"buckets": [], "note": "No resolved markets available for calibration"}

    # Map resolution to binary: 1 = yes, 0 = no
    resolved["outcome_binary"] = resolved["resolution"].apply(_resolution_to_binary)
    resolved = resolved.dropna(subset=["outcome_binary", "outcome_yes_price"])
    resolved["implied_prob"] = resolved["outcome_yes_price"].astype(float)

    if resolved.empty:
        return {"buckets": [], "note": "No resolved markets with valid prices"}

    buckets = []
    for low, high in config.CALIBRATION_BUCKETS:
        mask = (resolved["implied_prob"] >= low) & (resolved["implied_prob"] < high)
        subset = resolved[mask]
        if len(subset) > 0:
            actual_rate = subset["outcome_binary"].mean()
            midpoint = (low + high) / 2
            buckets.append({
                "range": f"{low:.1f}-{high:.1f}",
                "midpoint": midpoint,
                "count": int(len(subset)),
                "implied_prob_avg": float(subset["implied_prob"].mean()),
                "actual_rate": float(actual_rate),
                "calibration_error": float(abs(actual_rate - subset["implied_prob"].mean())),
            })

    total_error = np.mean([b["calibration_error"] for b in buckets]) if buckets else 0
    return {
        "buckets": buckets,
        "mean_calibration_error": float(total_error),
        "total_resolved_markets": int(len(resolved)),
    }


def _resolution_to_binary(res) -> float | None:
    """Convert resolution string to binary outcome."""
    if pd.isna(res):
        return None
    res_str = str(res).lower().strip()
    if res_str in ("yes", "1", "1.0", "true"):
        return 1.0
    if res_str in ("no", "0", "0.0", "false"):
        return 0.0
    return None


def compute_forecast_errors(markets_df: pd.DataFrame) -> dict:
    """
    Compute forecast error metrics for resolved markets.
    """
    resolved = markets_df[markets_df["resolved"] == True].copy()
    if resolved.empty:
        return {"note": "No resolved markets for error analysis"}

    resolved["outcome_binary"] = resolved["resolution"].apply(_resolution_to_binary)
    resolved = resolved.dropna(subset=["outcome_binary", "outcome_yes_price"])
    resolved["implied_prob"] = resolved["outcome_yes_price"].astype(float)

    if resolved.empty:
        return {"note": "No resolved markets with valid data"}

    errors = resolved["implied_prob"] - resolved["outcome_binary"]
    abs_errors = errors.abs()

    return {
        "count": int(len(resolved)),
        "mean_absolute_error": float(abs_errors.mean()),
        "mean_squared_error": float((errors ** 2).mean()),
        "brier_score": float((errors ** 2).mean()),
        "mean_bias": float(errors.mean()),  # positive = overconfident on yes
        "median_absolute_error": float(abs_errors.median()),
    }


def identify_mispriced_markets(markets_df: pd.DataFrame, snapshots_df: pd.DataFrame) -> list[dict]:
    """
    Identify potentially mispriced markets based on:
    - Extreme prices (very high/low implied probability on uncertain events)
    - Large recent price moves (potential slow reactions)
    - Low liquidity with high volume (potential arbitrage)
    """
    active = markets_df[markets_df["resolved"] != True].copy()
    if active.empty:
        return []

    mispriced = []

    for _, row in active.iterrows():
        signals = []
        yes_price = row.get("outcome_yes_price")
        volume = row.get("volume", 0)
        liquidity = row.get("liquidity", 0)

        if yes_price is None or pd.isna(yes_price):
            continue

        yes_price = float(yes_price)

        # Signal: extreme near-boundary prices with meaningful volume
        if volume and float(volume) > 1000:
            if 0.02 < yes_price < 0.10:
                signals.append({"type": "low_price_high_volume", "detail": f"Yes={yes_price:.3f}, Vol={volume}"})
            elif 0.90 < yes_price < 0.98:
                signals.append({"type": "high_price_high_volume", "detail": f"Yes={yes_price:.3f}, Vol={volume}"})

        # Signal: low liquidity relative to volume (thin orderbook)
        if volume and liquidity and float(liquidity) > 0:
            vol_liq_ratio = float(volume) / float(liquidity)
            if vol_liq_ratio > 3:
                signals.append({
                    "type": "thin_liquidity",
                    "detail": f"Volume/Liquidity={vol_liq_ratio:.1f}",
                })

        # Signal: check for large price moves in snapshots
        if not snapshots_df.empty:
            market_snaps = snapshots_df[
                (snapshots_df["source"] == row["source"]) &
                (snapshots_df["market_id"] == row["market_id"])
            ].sort_values("snapshot_time")

            if len(market_snaps) >= 2:
                prices = market_snaps["outcome_yes_price"].dropna().astype(float)
                if len(prices) >= 2:
                    recent_change = abs(prices.iloc[-1] - prices.iloc[-2])
                    if recent_change > 0.15:
                        signals.append({
                            "type": "large_price_move",
                            "detail": f"Change={recent_change:.3f} in last snapshot",
                        })

                    # Trend detection
                    if len(prices) >= 3:
                        diffs = prices.diff().dropna()
                        if (diffs > 0).all():
                            signals.append({"type": "consistent_uptrend", "detail": f"Trending up over {len(prices)} snapshots"})
                        elif (diffs < 0).all():
                            signals.append({"type": "consistent_downtrend", "detail": f"Trending down over {len(prices)} snapshots"})

        if signals:
            mispriced.append({
                "source": row["source"],
                "market_id": row["market_id"],
                "question": row.get("question", ""),
                "current_yes_price": yes_price,
                "signals": signals,
            })

    return mispriced


def detect_cross_platform_divergences(markets_df: pd.DataFrame) -> list[dict]:
    """
    Find markets covering the same event on different platforms with divergent prices.
    This is the classic arbitrage signal.
    """
    if markets_df.empty or "source" not in markets_df.columns:
        return []

    active = markets_df[markets_df["resolved"] != True].copy()
    if active.empty:
        return []

    # Group by similar questions (fuzzy match on city + date patterns)
    import re
    divergences = []

    poly = active[active["source"] == "polymarket"]
    kalshi = active[active["source"] == "kalshi"]

    if poly.empty or kalshi.empty:
        return []

    # Extract city and threshold from questions for matching
    def _extract_key(q):
        q = q.lower()
        cities = ["new york", "nyc", "chicago", "miami", "los angeles", "la", "denver", "seattle", "boston"]
        city = None
        for c in cities:
            if c in q:
                city = c
                break
        date_match = re.search(r'march\s+(\d+)', q)
        date = date_match.group(1) if date_match else None
        return (city, date) if city and date else None

    poly_by_key = {}
    for _, row in poly.iterrows():
        key = _extract_key(row["question"])
        if key:
            poly_by_key[key] = row

    for _, row in kalshi.iterrows():
        key = _extract_key(row["question"])
        if key and key in poly_by_key:
            poly_row = poly_by_key[key]
            poly_price = float(poly_row.get("outcome_yes_price", 0))
            kalshi_price = float(row.get("outcome_yes_price", 0))
            spread = abs(poly_price - kalshi_price)

            if spread > 0.01:  # More than 1 cent divergence
                divergences.append({
                    "city": key[0],
                    "date": key[1],
                    "polymarket_yes": poly_price,
                    "kalshi_yes": kalshi_price,
                    "spread": round(spread, 3),
                    "polymarket_question": poly_row["question"],
                    "kalshi_question": row["question"],
                    "direction": "Polymarket higher" if poly_price > kalshi_price else "Kalshi higher",
                })

    divergences.sort(key=lambda x: x["spread"], reverse=True)
    return divergences


def detect_biases(markets_df: pd.DataFrame) -> dict:
    """Detect seasonal and regional biases in weather markets."""
    resolved = markets_df[markets_df["resolved"] == True].copy()
    if resolved.empty:
        return {"note": "Insufficient resolved markets for bias detection"}

    resolved["outcome_binary"] = resolved["resolution"].apply(_resolution_to_binary)
    resolved = resolved.dropna(subset=["outcome_binary", "outcome_yes_price"])
    resolved["implied_prob"] = resolved["outcome_yes_price"].astype(float)
    resolved["error"] = resolved["implied_prob"] - resolved["outcome_binary"]

    biases = {}

    # Regional bias (by detecting city names)
    from fetch_weather import CITY_COORDS
    for city in CITY_COORDS:
        mask = resolved["question"].str.lower().str.contains(city, na=False)
        subset = resolved[mask]
        if len(subset) >= 3:
            biases[f"region_{city}"] = {
                "count": int(len(subset)),
                "mean_error": float(subset["error"].mean()),
                "direction": "overestimates yes" if subset["error"].mean() > 0 else "underestimates yes",
            }

    # Keyword-based category bias
    categories = {
        "temperature": ["temperature", "temp", "hot", "cold", "heat", "freeze", "warm"],
        "precipitation": ["rain", "snow", "precipitation", "rainfall", "snowfall"],
        "storms": ["hurricane", "tornado", "storm", "cyclone", "typhoon"],
    }

    for cat_name, keywords in categories.items():
        mask = resolved["question"].str.lower().apply(
            lambda q: any(kw in q for kw in keywords) if isinstance(q, str) else False
        )
        subset = resolved[mask]
        if len(subset) >= 3:
            biases[f"category_{cat_name}"] = {
                "count": int(len(subset)),
                "mean_error": float(subset["error"].mean()),
                "brier_score": float((subset["error"] ** 2).mean()),
            }

    return biases


def run_full_analysis(markets_df: pd.DataFrame, snapshots_df: pd.DataFrame, outcomes_df: pd.DataFrame) -> dict:
    """Run all analyses and save results."""
    has_markets = not markets_df.empty and "resolved" in markets_df.columns

    if has_markets:
        total = int(len(markets_df))
        active = int(len(markets_df[markets_df["resolved"] != True]))
        resolved_count = int(len(markets_df[markets_df["resolved"] == True]))
        by_source = markets_df["source"].value_counts().to_dict()
    else:
        total = active = resolved_count = 0
        by_source = {}

    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "market_counts": {
            "total": total,
            "active": active,
            "resolved": resolved_count,
            "by_source": by_source,
        },
        "calibration": compute_calibration(markets_df, outcomes_df) if has_markets else {"note": "No market data"},
        "forecast_errors": compute_forecast_errors(markets_df) if has_markets else {"note": "No market data"},
        "mispriced_markets": identify_mispriced_markets(markets_df, snapshots_df) if has_markets else [],
        "biases": detect_biases(markets_df) if has_markets else {"note": "No market data"},
        "cross_platform_divergences": detect_cross_platform_divergences(markets_df) if has_markets else [],
        "snapshot_count": int(len(snapshots_df)) if not snapshots_df.empty else 0,
        "outcome_count": int(len(outcomes_df)) if not outcomes_df.empty else 0,
    }

    # Save results
    with open(config.ANALYSIS_RESULTS, "w") as f:
        json.dump(results, f, indent=2, default=str)
    logger.info("Analysis results saved to %s", config.ANALYSIS_RESULTS)

    return results
