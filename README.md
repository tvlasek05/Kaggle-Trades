# Weather Prediction Market Tracker

Automated pipeline that collects weather prediction market data, tracks actual outcomes, and identifies trading opportunities through calibration analysis.

## What It Does

Each session:
1. **Pulls** latest weather prediction market data from Kalshi and Polymarket APIs
2. **Appends** new data to persistent CSV datasets in `data/`
3. **Maintains** rolling time series of prices for each market
4. **Fetches** actual weather outcomes from Open-Meteo (free, no API key)
5. **Records** resolved market outcomes and actual results
6. **Computes**: implied probability vs actual outcome, forecast error (Brier/log loss), calibration by probability bucket
7. **Identifies**: mispriced markets, slow market reactions, seasonal/regional biases
8. **Outputs** concise summary of insights and potential trading opportunities

## Quick Start

```bash
pip install -r requirements.txt

# Generate seed data for testing (when APIs unavailable)
python seed_sample_data.py

# Run a full session (fetches live data + analyzes)
python run_session.py

# Run analysis only on existing data
python run_session.py --offline
```

## Project Structure

```
run_session.py         # Main orchestrator - run this each session
fetch_markets.py       # Kalshi + Polymarket weather market fetcher
fetch_weather.py       # Open-Meteo actual weather data fetcher
analyze.py             # Calibration, errors, mispricings, bias analysis
config.py              # All configuration and paths
seed_sample_data.py    # Generate realistic demo data
data/
  market_snapshots.csv   # Rolling price time series
  weather_actuals.csv    # Actual weather observations
  resolved_markets.csv   # Markets with known outcomes
  analysis_results.json  # Most recent analysis results
analysis/
  calibration.csv        # Calibration by probability bucket
  insights.log           # Running log of detected insights
```

## Data Sources

| Source | API | Auth Required |
|--------|-----|---------------|
| Kalshi | api.kalshi.com | No (public markets) |
| Polymarket | gamma-api.polymarket.com | No |
| Open-Meteo | api.open-meteo.com | No |

## Analysis Metrics

- **Brier Score**: Mean squared error of probability forecasts (lower = better, 0 = perfect)
- **Log Loss**: Cross-entropy loss (penalizes confident wrong predictions)
- **Calibration**: Do markets priced at X% resolve YES X% of the time?
- **Mispricing Signals**: Compare market implied probability to weather-forecast-based model estimate
- **Slow Reactions**: Markets with large price moves over extended time periods
- **Bias Detection**: Systematic over/under-confidence by source, region, or season

## Tracked Cities

New York, Chicago, Los Angeles, Miami (matching Kalshi weather contract series).
