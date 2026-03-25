"""Configuration for weather prediction market tracker."""

import os

# --- Data Sources ---

# Polymarket CLOB API (no auth needed for reads)
POLYMARKET_API = "https://clob.polymarket.com"
POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"

# Kalshi API (public read endpoints)
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"

# Open-Meteo (free, no auth)
OPEN_METEO_API = "https://api.open-meteo.com/v1"
OPEN_METEO_HISTORICAL = "https://archive-api.open-meteo.com/v1/archive"

# --- Weather search terms for finding markets ---
WEATHER_KEYWORDS = [
    "temperature", "weather", "hurricane", "tornado", "snow",
    "rainfall", "heat", "cold", "storm", "drought", "flood",
    "celsius", "fahrenheit", "climate", "el nino", "la nina",
    "wildfire", "wind", "precipitation",
]

# --- File paths ---
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
ANALYSIS_DIR = os.path.join(os.path.dirname(__file__), "analysis")

MARKETS_FILE = os.path.join(DATA_DIR, "markets.csv")
PRICES_FILE = os.path.join(DATA_DIR, "price_history.csv")
RESOLUTIONS_FILE = os.path.join(DATA_DIR, "resolutions.csv")
WEATHER_ACTUALS_FILE = os.path.join(DATA_DIR, "weather_actuals.csv")
ANALYSIS_FILE = os.path.join(ANALYSIS_DIR, "calibration_analysis.csv")
OPPORTUNITIES_FILE = os.path.join(ANALYSIS_DIR, "opportunities.csv")
SUMMARY_FILE = os.path.join(ANALYSIS_DIR, "latest_summary.txt")

# --- Analysis parameters ---
CALIBRATION_BUCKETS = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
MISPRICING_THRESHOLD = 0.15  # Flag markets where implied prob differs from model by >15%
STALE_HOURS = 12  # Flag markets with no price movement for this many hours

# --- Request settings ---
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds
