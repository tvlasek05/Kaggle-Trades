"""Configuration for weather prediction market tracker."""

import os

# Directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
RESULTS_DIR = os.path.join(BASE_DIR, "results")

# Polymarket Gamma API (market discovery, no auth required)
POLYMARKET_GAMMA_URL = "https://gamma-api.polymarket.com"
# Polymarket CLOB API (orderbook/prices)
POLYMARKET_CLOB_URL = "https://clob.polymarket.com"

# Kalshi API (public read access)
KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Open-Meteo (free, no auth)
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Weather-related search keywords for filtering prediction markets
WEATHER_KEYWORDS = [
    "temperature", "weather", "rain", "snow", "hurricane", "tornado",
    "heat", "cold", "freeze", "drought", "flood", "storm", "celsius",
    "fahrenheit", "precipitation", "wind", "climate", "el nino", "la nina",
    "arctic", "ice", "heatwave", "heat wave", "winter storm", "tropical",
    "cyclone", "typhoon", "wildfire", "record high", "record low",
    "above average", "below average", "hottest", "coldest", "warmest",
]

# Data files
MARKETS_FILE = os.path.join(DATA_DIR, "markets.csv")
PRICES_FILE = os.path.join(DATA_DIR, "price_history.csv")
OUTCOMES_FILE = os.path.join(DATA_DIR, "outcomes.csv")
ANALYSIS_FILE = os.path.join(RESULTS_DIR, "analysis.csv")
CALIBRATION_FILE = os.path.join(RESULTS_DIR, "calibration.csv")
SUMMARY_FILE = os.path.join(RESULTS_DIR, "summary.txt")

# Calibration bucket edges
CALIBRATION_BUCKETS = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

# Request settings
REQUEST_TIMEOUT = 30
REQUEST_DELAY = 0.5  # seconds between API calls to be respectful
