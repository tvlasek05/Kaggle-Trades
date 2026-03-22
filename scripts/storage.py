"""
Persistent data storage layer using CSV files.

Stores:
- market_snapshots.csv: Rolling time series of market prices
- weather_actuals.csv: Actual weather outcomes
- resolved_markets.csv: Markets that have resolved with outcomes
"""

import os
import pandas as pd
from datetime import datetime, timezone

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def _csv_path(name):
    return os.path.join(DATA_DIR, f"{name}.csv")


def append_market_snapshots(markets):
    """Append new market price snapshots to the rolling time series."""
    if not markets:
        return 0
    ensure_data_dir()
    path = _csv_path("market_snapshots")
    df_new = pd.DataFrame(markets)

    if os.path.exists(path):
        df_existing = pd.read_csv(path)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new

    df_combined.to_csv(path, index=False)
    return len(df_new)


def append_weather_actuals(records):
    """Append new weather observations to the actuals dataset."""
    if not records:
        return 0
    ensure_data_dir()
    path = _csv_path("weather_actuals")
    df_new = pd.DataFrame(records)

    if os.path.exists(path):
        df_existing = pd.read_csv(path)
        # Deduplicate on city + date (keep latest fetch)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(
            subset=["city", "date"], keep="last"
        )
    else:
        df_combined = df_new

    df_combined.to_csv(path, index=False)
    return len(df_new)


def append_resolved_markets(markets):
    """Store resolved market outcomes."""
    if not markets:
        return 0
    ensure_data_dir()
    path = _csv_path("resolved_markets")
    df_new = pd.DataFrame(markets)

    if os.path.exists(path):
        df_existing = pd.read_csv(path)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(
            subset=["source", "market_id"], keep="last"
        )
    else:
        df_combined = df_new

    df_combined.to_csv(path, index=False)
    return len(df_new)


def load_market_snapshots():
    """Load full market snapshot history."""
    path = _csv_path("market_snapshots")
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()


def load_weather_actuals():
    """Load weather actuals dataset."""
    path = _csv_path("weather_actuals")
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()


def load_resolved_markets():
    """Load resolved markets dataset."""
    path = _csv_path("resolved_markets")
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()


def save_analysis_results(results, filename="analysis_results"):
    """Save analysis results to CSV."""
    ensure_data_dir()
    path = _csv_path(filename)
    df = pd.DataFrame(results) if isinstance(results, list) else results
    df.to_csv(path, index=False)
    return path
