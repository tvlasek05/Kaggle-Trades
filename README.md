# Weather Prediction Market Tracker

Tracks weather prediction markets (Kalshi, Polymarket), compares implied probabilities to actual weather outcomes, and identifies mispricing opportunities.

## Quick Start

```bash
pip install -r requirements.txt

# Generate seed data for testing
python generate_seed_data.py

# Run a full session (fetches live data + analyzes)
python run_session.py

# Run analysis only on existing data (no API calls)
python run_session.py --offline
```

## Data Sources

- **Kalshi**: Temperature bracket markets (NYC, Chicago, LA, Miami), rain, snow, hurricane
- **Polymarket**: Weather/climate tagged prediction markets
- **Open-Meteo**: Free weather forecasts and historical actuals (no API key needed)

## What It Does

Each session:
1. Pulls latest market prices from Kalshi and Polymarket APIs
2. Fetches weather forecasts and recent actuals from Open-Meteo
3. Appends new data to persistent CSVs in `data/`
4. Matches resolved markets with actual weather outcomes
5. Computes calibration, Brier scores, and forecast errors
6. Identifies mispriced markets, slow reactions, and regional/seasonal biases
7. Outputs a summary with potential trading opportunities

## Output Files

- `data/market_snapshots.csv` - Rolling time series of market prices
- `data/weather_actuals.csv` - Actual weather outcomes by city/date
- `data/resolved_markets.csv` - Matched market results with actuals
- `data/analysis_results.json` - Full analysis output

## Project Structure

| File | Purpose |
|---|---|
| `run_session.py` | Main entry point - orchestrates fetch + analysis |
| `fetch_markets.py` | Kalshi and Polymarket API clients |
| `fetch_weather.py` | Open-Meteo weather data client |
| `analyze.py` | Calibration, mispricing, bias detection |
| `config.py` | Configuration (API URLs, cities, thresholds) |
| `generate_seed_data.py` | Generate realistic test data |
