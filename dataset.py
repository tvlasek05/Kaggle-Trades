"""Persistent dataset management for weather prediction market data."""

import logging
import pandas as pd
from datetime import datetime, timezone

import config

logger = logging.getLogger(__name__)


def load_markets() -> pd.DataFrame:
    """Load existing markets dataset."""
    if config.MARKETS_CSV.exists():
        return pd.read_csv(config.MARKETS_CSV)
    return pd.DataFrame()


def load_snapshots() -> pd.DataFrame:
    """Load existing price snapshots."""
    if config.SNAPSHOTS_CSV.exists():
        return pd.read_csv(config.SNAPSHOTS_CSV)
    return pd.DataFrame()


def load_outcomes() -> pd.DataFrame:
    """Load existing outcomes dataset."""
    if config.OUTCOMES_CSV.exists():
        return pd.read_csv(config.OUTCOMES_CSV)
    return pd.DataFrame()


def save_markets(df: pd.DataFrame):
    """Save markets dataset."""
    df.to_csv(config.MARKETS_CSV, index=False)
    logger.info("Saved %d markets to %s", len(df), config.MARKETS_CSV)


def save_snapshots(df: pd.DataFrame):
    """Save price snapshots."""
    df.to_csv(config.SNAPSHOTS_CSV, index=False)
    logger.info("Saved %d snapshots to %s", len(df), config.SNAPSHOTS_CSV)


def save_outcomes(df: pd.DataFrame):
    """Save outcomes dataset."""
    df.to_csv(config.OUTCOMES_CSV, index=False)
    logger.info("Saved %d outcomes to %s", len(df), config.OUTCOMES_CSV)


def update_markets(new_markets_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge new market data with existing dataset.

    - Updates existing markets with latest prices/status
    - Adds new markets not seen before
    - Preserves historical records
    """
    if new_markets_df.empty:
        return load_markets()

    existing = load_markets()

    if existing.empty:
        save_markets(new_markets_df)
        return new_markets_df

    # Use source + market_id as composite key
    new_markets_df["_key"] = new_markets_df["source"] + ":" + new_markets_df["market_id"]
    existing["_key"] = existing["source"] + ":" + existing["market_id"]

    # Update existing records
    existing_keys = set(existing["_key"])
    new_keys = set(new_markets_df["_key"])

    # Markets only in new data
    added = new_markets_df[~new_markets_df["_key"].isin(existing_keys)]

    # Markets in both - update with new data
    updated_keys = existing_keys & new_keys
    unchanged = existing[~existing["_key"].isin(updated_keys)]
    updates = new_markets_df[new_markets_df["_key"].isin(updated_keys)]

    result = pd.concat([unchanged, updates, added], ignore_index=True)
    result.drop(columns=["_key"], inplace=True)

    save_markets(result)
    logger.info(
        "Markets updated: %d existing, %d updated, %d new",
        len(unchanged), len(updates), len(added),
    )
    return result


def append_price_snapshots(markets_df: pd.DataFrame) -> pd.DataFrame:
    """
    Append current prices as a new time-series snapshot.
    """
    if markets_df.empty:
        return load_snapshots()

    now = datetime.now(timezone.utc).isoformat()

    snapshot_records = []
    for _, row in markets_df.iterrows():
        snapshot_records.append({
            "source": row.get("source", ""),
            "market_id": row.get("market_id", ""),
            "question": row.get("question", ""),
            "outcome_yes_price": row.get("outcome_yes_price"),
            "outcome_no_price": row.get("outcome_no_price"),
            "volume": row.get("volume", 0),
            "liquidity": row.get("liquidity", 0),
            "snapshot_time": now,
        })

    new_snapshots = pd.DataFrame(snapshot_records)
    existing = load_snapshots()
    combined = pd.concat([existing, new_snapshots], ignore_index=True)
    save_snapshots(combined)
    return combined


def update_outcomes(new_outcomes: list[dict]) -> pd.DataFrame:
    """Append new outcome records, avoiding duplicates."""
    if not new_outcomes:
        return load_outcomes()

    new_df = pd.DataFrame(new_outcomes)
    existing = load_outcomes()

    if existing.empty:
        save_outcomes(new_df)
        return new_df

    # Deduplicate by market_id + date
    new_df["_key"] = new_df["source"] + ":" + new_df["market_id"] + ":" + new_df["date"]
    existing["_key"] = existing["source"] + ":" + existing["market_id"] + ":" + existing["date"]

    novel = new_df[~new_df["_key"].isin(set(existing["_key"]))]
    combined = pd.concat([existing, novel], ignore_index=True)
    combined.drop(columns=["_key"], inplace=True)
    save_outcomes(combined)
    return combined
