"""Analyze weather prediction market calibration, mispricing, and biases."""

import csv
from collections import defaultdict
from datetime import datetime, timezone

import config


def load_csv(path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, "r") as f:
        return list(csv.DictReader(f))


def safe_float(val, default=None):
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def compute_implied_probability(market: dict) -> float | None:
    """Compute implied probability from market prices (Kalshi prices are in cents 0-100)."""
    last = safe_float(market.get("last_price"))
    if last is not None:
        return last / 100.0

    yes_bid = safe_float(market.get("yes_bid"))
    yes_ask = safe_float(market.get("yes_ask"))
    if yes_bid is not None and yes_ask is not None:
        return (yes_bid + yes_ask) / 200.0
    return None


def determine_actual_outcome(market: dict, outcomes: dict) -> int | None:
    """Determine if the market resolved YES (1) or NO (0).

    For settled markets, use the result field.
    For past markets with weather data, compare actual vs strike.
    """
    result = market.get("result", "").lower()
    if result == "yes":
        return 1
    elif result == "no":
        return 0

    # Infer from actual weather data
    event = market.get("event_ticker", "")
    target_date = market.get("target_date", "")
    key = f"{event}_{target_date}"
    outcome = outcomes.get(key)

    if not outcome:
        return None

    actual = safe_float(outcome.get("actual_value"))
    strike = safe_float(market.get("strike_value"))
    if actual is None or strike is None:
        return None

    series = market.get("series_ticker", "")
    # For high temperature markets: resolved YES if actual >= strike
    if "HIGH" in series:
        return 1 if actual >= strike else 0
    # For snow markets: resolved YES if snowfall >= strike
    if "SNOW" in series:
        return 1 if actual >= strike else 0

    return None


def calibration_analysis(records: list[dict]) -> list[dict]:
    """Compute calibration by probability bucket."""
    buckets = defaultdict(lambda: {"count": 0, "yes_count": 0, "prob_sum": 0.0})

    for r in records:
        prob = safe_float(r.get("implied_prob"))
        outcome = r.get("actual_outcome")
        if prob is None or outcome is None:
            continue
        outcome = int(outcome)

        # Find bucket
        bucket_idx = min(int(prob * 10), 9)
        lo = bucket_idx / 10.0
        hi = (bucket_idx + 1) / 10.0
        bucket_label = f"{lo:.1f}-{hi:.1f}"

        buckets[bucket_label]["count"] += 1
        buckets[bucket_label]["yes_count"] += outcome
        buckets[bucket_label]["prob_sum"] += prob

    results = []
    for label in sorted(buckets.keys()):
        b = buckets[label]
        if b["count"] == 0:
            continue
        avg_prob = b["prob_sum"] / b["count"]
        actual_rate = b["yes_count"] / b["count"]
        results.append({
            "bucket": label,
            "count": b["count"],
            "avg_implied_prob": round(avg_prob, 4),
            "actual_hit_rate": round(actual_rate, 4),
            "calibration_error": round(abs(avg_prob - actual_rate), 4),
        })
    return results


def find_mispriced_markets(records: list[dict]) -> list[dict]:
    """Identify markets where implied probability diverges significantly from outcomes."""
    mispriced = []
    for r in records:
        prob = safe_float(r.get("implied_prob"))
        outcome = r.get("actual_outcome")
        if prob is None or outcome is None:
            continue
        outcome = int(outcome)

        error = abs(prob - outcome)
        if error > config.MISPRICING_THRESHOLD:
            mispriced.append({
                "ticker": r["ticker"],
                "title": r.get("title", ""),
                "implied_prob": prob,
                "actual_outcome": outcome,
                "error": round(error, 4),
                "city": r.get("city", ""),
                "target_date": r.get("target_date", ""),
                "strike_value": r.get("strike_value", ""),
            })

    mispriced.sort(key=lambda x: x["error"], reverse=True)
    return mispriced


def detect_biases(records: list[dict]) -> dict:
    """Detect seasonal, regional, and directional biases."""
    biases = {
        "by_city": defaultdict(lambda: {"count": 0, "error_sum": 0.0, "bias_sum": 0.0}),
        "by_month": defaultdict(lambda: {"count": 0, "error_sum": 0.0, "bias_sum": 0.0}),
        "by_series": defaultdict(lambda: {"count": 0, "error_sum": 0.0, "bias_sum": 0.0}),
    }

    for r in records:
        prob = safe_float(r.get("implied_prob"))
        outcome = r.get("actual_outcome")
        if prob is None or outcome is None:
            continue
        outcome = int(outcome)

        error = abs(prob - outcome)
        bias = prob - outcome  # positive = market overpriced YES

        city = r.get("city", "unknown")
        biases["by_city"][city]["count"] += 1
        biases["by_city"][city]["error_sum"] += error
        biases["by_city"][city]["bias_sum"] += bias

        target_date = r.get("target_date", "")
        if len(target_date) >= 7:
            month = target_date[5:7]
            biases["by_month"][month]["count"] += 1
            biases["by_month"][month]["error_sum"] += error
            biases["by_month"][month]["bias_sum"] += bias

        series = r.get("series_ticker", "")
        biases["by_series"][series]["count"] += 1
        biases["by_series"][series]["error_sum"] += error
        biases["by_series"][series]["bias_sum"] += bias

    # Compute averages
    result = {}
    for category, data in biases.items():
        result[category] = {}
        for key, vals in sorted(data.items()):
            if vals["count"] == 0:
                continue
            result[category][key] = {
                "count": vals["count"],
                "avg_error": round(vals["error_sum"] / vals["count"], 4),
                "avg_bias": round(vals["bias_sum"] / vals["count"], 4),
            }
    return result


def detect_slow_reactions(prices: list[dict], markets: dict) -> list[dict]:
    """Identify markets where price moved slowly toward eventual outcome."""
    # Group price snapshots by ticker
    by_ticker = defaultdict(list)
    for p in prices:
        by_ticker[p["ticker"]].append(p)

    slow = []
    for ticker, snapshots in by_ticker.items():
        mkt = markets.get(ticker)
        if not mkt:
            continue
        result = mkt.get("result", "").lower()
        if result not in ("yes", "no"):
            continue

        final_value = 100 if result == "yes" else 0
        sorted_snaps = sorted(snapshots, key=lambda s: s.get("timestamp", ""))

        if len(sorted_snaps) < 2:
            continue

        # Check if early prices were far from final outcome
        early_price = safe_float(sorted_snaps[0].get("last_price"))
        late_price = safe_float(sorted_snaps[-1].get("last_price"))
        if early_price is None or late_price is None:
            continue

        early_dist = abs(early_price - final_value)
        late_dist = abs(late_price - final_value)

        if early_dist > 30 and late_dist < 15:
            slow.append({
                "ticker": ticker,
                "title": mkt.get("title", ""),
                "early_price": early_price,
                "late_price": late_price,
                "final_outcome": result,
                "price_move": round(late_price - early_price, 2),
                "snapshots": len(sorted_snaps),
            })

    slow.sort(key=lambda x: abs(x["price_move"]), reverse=True)
    return slow


def run(markets_dict: dict | None = None, outcomes_dict: dict | None = None):
    """Run full analysis pipeline."""
    # Load data
    markets_list = load_csv(config.MARKETS_CSV)
    if markets_dict is None:
        markets_dict = {m["ticker"]: m for m in markets_list}

    outcomes_list = load_csv(config.OUTCOMES_CSV)
    if outcomes_dict is None:
        outcomes_dict = {}
        for o in outcomes_list:
            key = f"{o['event_ticker']}_{o['target_date']}"
            outcomes_dict[key] = o

    prices_list = load_csv(config.PRICES_CSV)

    # Enrich markets with implied probability and actual outcome
    enriched = []
    for mkt in markets_list:
        prob = compute_implied_probability(mkt)
        outcome = determine_actual_outcome(mkt, outcomes_dict)
        row = dict(mkt)
        row["implied_prob"] = prob
        row["actual_outcome"] = outcome
        enriched.append(row)

    # Run analyses
    calibration = calibration_analysis(enriched)
    mispriced = find_mispriced_markets(enriched)
    biases = detect_biases(enriched)
    slow_reactions = detect_slow_reactions(prices_list, markets_dict)

    # Count stats
    total = len(enriched)
    resolved = sum(1 for e in enriched if e["actual_outcome"] is not None)
    open_markets = sum(1 for e in enriched if e.get("status", "").lower() in ("open", "active"))

    # Save analysis results
    save_analysis(enriched)

    # Generate summary
    summary = generate_summary(
        total, resolved, open_markets,
        calibration, mispriced, biases, slow_reactions,
    )
    with open(config.SUMMARY_TXT, "w") as f:
        f.write(summary)

    print(summary)
    return summary


def save_analysis(enriched: list[dict]):
    """Save enriched market data with analysis columns."""
    if not enriched:
        return
    fieldnames = [
        "ticker", "event_ticker", "series_ticker", "title",
        "strike_value", "target_date", "city",
        "last_price", "volume", "status", "result",
        "implied_prob", "actual_outcome",
    ]
    with open(config.ANALYSIS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in enriched:
            writer.writerow(row)


def generate_summary(
    total, resolved, open_markets,
    calibration, mispriced, biases, slow_reactions,
) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"=== Weather Prediction Market Analysis ===",
        f"Generated: {now}",
        f"",
        f"DATASET OVERVIEW",
        f"  Total markets tracked: {total}",
        f"  Resolved with outcomes: {resolved}",
        f"  Currently open: {open_markets}",
        f"",
    ]

    # Calibration
    lines.append("CALIBRATION BY PROBABILITY BUCKET")
    if calibration:
        lines.append(f"  {'Bucket':<10} {'Count':>6} {'Avg Prob':>10} {'Hit Rate':>10} {'Error':>8}")
        lines.append(f"  {'-'*46}")
        for c in calibration:
            lines.append(
                f"  {c['bucket']:<10} {c['count']:>6} "
                f"{c['avg_implied_prob']:>10.1%} {c['actual_hit_rate']:>10.1%} "
                f"{c['calibration_error']:>8.1%}"
            )
    else:
        lines.append("  No resolved markets with probability data yet.")
    lines.append("")

    # Mispriced
    lines.append(f"MISPRICED MARKETS (error > {config.MISPRICING_THRESHOLD:.0%})")
    if mispriced:
        for m in mispriced[:10]:
            outcome_str = "YES" if m["actual_outcome"] == 1 else "NO"
            lines.append(
                f"  {m['ticker']}: implied={m['implied_prob']:.1%}, "
                f"actual={outcome_str}, error={m['error']:.1%} "
                f"({m['city']} {m['target_date']})"
            )
        if len(mispriced) > 10:
            lines.append(f"  ... and {len(mispriced) - 10} more")
    else:
        lines.append("  None identified yet.")
    lines.append("")

    # Biases
    lines.append("BIAS ANALYSIS")
    for category, data in biases.items():
        if not data:
            continue
        label = category.replace("by_", "By ").title()
        lines.append(f"  {label}:")
        for key, vals in data.items():
            direction = "overprices YES" if vals["avg_bias"] > 0.02 else (
                "underprices YES" if vals["avg_bias"] < -0.02 else "well-calibrated"
            )
            lines.append(
                f"    {key}: n={vals['count']}, avg_error={vals['avg_error']:.1%}, "
                f"bias={vals['avg_bias']:+.1%} ({direction})"
            )
    if not any(biases.values()):
        lines.append("  Insufficient data for bias detection.")
    lines.append("")

    # Slow reactions
    lines.append("SLOW MARKET REACTIONS")
    if slow_reactions:
        for s in slow_reactions[:5]:
            lines.append(
                f"  {s['ticker']}: {s['early_price']}c -> {s['late_price']}c "
                f"(resolved {s['final_outcome'].upper()}, {s['snapshots']} snapshots)"
            )
    else:
        lines.append("  No slow reaction patterns detected (need multiple price snapshots).")
    lines.append("")

    # Trading opportunities
    lines.append("POTENTIAL TRADING OPPORTUNITIES")
    lines.append("  (Based on detected biases and mispricings)")
    if biases.get("by_city"):
        for city, vals in biases["by_city"].items():
            if vals["count"] >= 5 and abs(vals["avg_bias"]) > 0.05:
                if vals["avg_bias"] > 0:
                    lines.append(f"  -> {city}: Market tends to overprice YES by {vals['avg_bias']:.1%}. Consider selling YES.")
                else:
                    lines.append(f"  -> {city}: Market tends to underprice YES by {abs(vals['avg_bias']):.1%}. Consider buying YES.")
    if not mispriced and not any(
        v["count"] >= 5 and abs(v["avg_bias"]) > 0.05
        for data in biases.values()
        for v in data.values()
    ):
        lines.append("  No clear opportunities yet. More data needed.")
    lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    run()
