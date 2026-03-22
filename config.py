"""Configuration for weather prediction market tracker."""

import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
MARKETS_CSV = DATA_DIR / "markets.csv"
SNAPSHOTS_CSV = DATA_DIR / "price_snapshots.csv"
OUTCOMES_CSV = DATA_DIR / "outcomes.csv"
ANALYSIS_DIR = BASE_DIR / "analysis"
ANALYSIS_RESULTS = ANALYSIS_DIR / "results.json"

# Polymarket CLOB API (no auth required for public reads)
POLYMARKET_BASE_URL = "https://clob.polymarket.com"
POLYMARKET_GAMMA_URL = "https://gamma-api.polymarket.com"

# Kalshi API (public market data, no auth for reads)
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Open-Meteo (free, no auth)
OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1"

# Weather keywords to filter prediction markets
WEATHER_KEYWORDS = [
    "temperature", "weather", "hurricane", "tornado", "snow", "rain",
    "heat", "cold", "freeze", "drought", "flood", "storm", "celsius",
    "fahrenheit", "climate", "wildfire", "el nino", "la nina",
    "hottest", "coldest", "warmest", "record high", "record low",
    "precipitation", "snowfall", "rainfall", "heatwave", "heat wave",
    "arctic", "polar vortex", "wind", "monsoon", "typhoon", "cyclone",
]

# Calibration buckets
CALIBRATION_BUCKETS = [
    (0.0, 0.1), (0.1, 0.2), (0.2, 0.3), (0.3, 0.4), (0.4, 0.5),
    (0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.01),
]

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
ANALYSIS_DIR.mkdir(exist_ok=True)
