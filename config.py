"""Configuration for weather prediction market analysis."""

import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# Data files
MARKETS_CSV = DATA_DIR / "markets.csv"
PRICES_CSV = DATA_DIR / "prices.csv"
OUTCOMES_CSV = DATA_DIR / "outcomes.csv"
ANALYSIS_CSV = OUTPUT_DIR / "analysis.csv"
SUMMARY_TXT = OUTPUT_DIR / "summary.txt"

# Kalshi API (canonical trading endpoint)
KALSHI_BASE_URL = "https://trading-api.kalshi.com/trade-api/v2"
KALSHI_WEATHER_SERIES = [
    "KXHIGHNY",    # NYC high temperature
    "KXHIGHCHI",   # Chicago high temperature
    "KXHIGHLA",    # LA high temperature
    "KXHIGHMIA",   # Miami high temperature
    "KXHIGHDAL",   # Dallas high temperature
    "KXHIGHDEN",   # Denver high temperature
    "KXSNOWNYC",   # NYC snowfall
    "KXSNOWCHI",   # Chicago snowfall
    "KXSNOWBOS",   # Boston snowfall
    "KXRAIN",      # Rainfall events
    "KXHURRICANE", # Hurricane events
]

# Open-Meteo API
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

# City coordinates for weather verification
CITY_COORDS = {
    "NY":  {"lat": 40.7128, "lon": -74.0060, "tz": "America/New_York"},
    "CHI": {"lat": 41.8781, "lon": -87.6298, "tz": "America/Chicago"},
    "LA":  {"lat": 34.0522, "lon": -118.2437, "tz": "America/Los_Angeles"},
    "MIA": {"lat": 25.7617, "lon": -80.1918, "tz": "America/New_York"},
    "DAL": {"lat": 32.7767, "lon": -96.7970, "tz": "America/Chicago"},
    "DEN": {"lat": 39.7392, "lon": -104.9903, "tz": "America/Denver"},
    "BOS": {"lat": 42.3601, "lon": -71.0589, "tz": "America/New_York"},
}

# Map series tickers to cities and weather variables
SERIES_CITY_MAP = {
    "KXHIGHNY": "NY",
    "KXHIGHCHI": "CHI",
    "KXHIGHLA": "LA",
    "KXHIGHMIA": "MIA",
    "KXHIGHDAL": "DAL",
    "KXHIGHDEN": "DEN",
    "KXSNOWNYC": "NY",
    "KXSNOWCHI": "CHI",
    "KXSNOWBOS": "BOS",
}

SERIES_VARIABLE_MAP = {
    "KXHIGHNY": "temperature_2m_max",
    "KXHIGHCHI": "temperature_2m_max",
    "KXHIGHLA": "temperature_2m_max",
    "KXHIGHMIA": "temperature_2m_max",
    "KXHIGHDAL": "temperature_2m_max",
    "KXHIGHDEN": "temperature_2m_max",
    "KXSNOWNYC": "snowfall_sum",
    "KXSNOWCHI": "snowfall_sum",
    "KXSNOWBOS": "snowfall_sum",
}

# Analysis settings
CALIBRATION_BUCKETS = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
MISPRICING_THRESHOLD = 0.15  # flag markets where implied prob differs from model by >15%
