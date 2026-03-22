"""Configuration for weather prediction market tracker."""

# Kalshi API
KALSHI_BASE_URL = "https://trading-api.kalshi.com/trade-api/v2"
KALSHI_WEATHER_SERIES = [
    "KXHIGHNY",   # NYC daily high temperature
    "KXHIGHCHI",  # Chicago daily high temperature
    "KXHIGHLAX",  # LA daily high temperature
    "KXHIGHMI",   # Miami daily high temperature
    "KXRAIN",     # NYC rain
    "KXSNOW",     # Snowfall
    "KXHURR",     # Hurricane
]

# Polymarket API
POLYMARKET_GAMMA_URL = "https://gamma-api.polymarket.com"
POLYMARKET_WEATHER_TAGS = ["weather", "climate"]
POLYMARKET_WEATHER_KEYWORDS = ["temperature", "hurricane", "storm", "rain", "snow", "weather"]

# Open-Meteo API (free, no key required)
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# City coordinates for weather verification
CITIES = {
    "NYC": {"lat": 40.71, "lon": -74.01},
    "CHI": {"lat": 41.88, "lon": -87.63},
    "LAX": {"lat": 34.05, "lon": -118.24},
    "MIA": {"lat": 25.76, "lon": -80.19},
}

# Map Kalshi series to cities
SERIES_CITY_MAP = {
    "KXHIGHNY": "NYC",
    "KXHIGHCHI": "CHI",
    "KXHIGHLAX": "LAX",
    "KXHIGHMI": "MIA",
    "KXRAIN": "NYC",
    "KXSNOW": "NYC",
}

# Data paths
DATA_DIR = "data"
MARKET_SNAPSHOTS_FILE = "data/market_snapshots.csv"
RESOLVED_MARKETS_FILE = "data/resolved_markets.csv"
WEATHER_ACTUALS_FILE = "data/weather_actuals.csv"
ANALYSIS_FILE = "data/analysis_results.json"

# Calibration buckets
CALIBRATION_BUCKETS = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

# Request settings
REQUEST_TIMEOUT = 15
REQUEST_RETRIES = 3
RETRY_BACKOFF = 2  # seconds
