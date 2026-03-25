# Weather Prediction Market Tracker

Automated pipeline that collects weather prediction market data from Kalshi and Polymarket, tracks actual outcomes via Open-Meteo, and identifies trading opportunities through calibration analysis.

## Quick Start

```bash
pip install -r requirements.txt

# Live mode (fetches from real APIs)
python3 run_session.py

# Demo mode (generates sample data for testing)
python3 run_session.py --demo
```

## What it does each session

1. **Collect** - Pulls weather prediction market data from Polymarket and Kalshi APIs
2. **Store** - Appends to persistent CSV datasets in `data/`
3. **Track** - Maintains rolling price time series per market
4. **Verify** - Fetches actual weather outcomes from Open-Meteo for resolved markets
5. **Analyze** - Computes calibration by probability bucket, forecast errors, Brier scores
6. **Identify** - Flags mispriced markets, slow reactions, seasonal/regional biases
7. **Report** - Outputs concise summary with trading opportunities to `results/summary.txt`

## Files

| File | Purpose |
|------|---------|
| `run_session.py` | Main entry point - run each session |
| `collector.py` | Polymarket + Kalshi data collection |
| `weather.py` | Open-Meteo weather verification |
| `analyzer.py` | Calibration, mispricing, bias detection |
| `config.py` | Configuration and file paths |
| `demo_data.py` | Generate sample data for testing |
| `data/markets.csv` | All tracked markets (latest snapshot) |
| `data/price_history.csv` | Rolling price time series |
| `data/outcomes.csv` | Verified weather outcomes |
| `results/analysis.csv` | Mispricing and bias findings |
| `results/calibration.csv` | Calibration by probability bucket |
| `results/summary.txt` | Human-readable session summary |

## Data Sources

| Source | API | Auth Required |
|--------|-----|---------------|
| Kalshi | trading-api.kalshi.com | No (public markets) |
| Polymarket | gamma-api.polymarket.com | No |
| Open-Meteo | api.open-meteo.com | No |

## Analysis Metrics

- **Brier Score**: Mean squared error of probability forecasts (lower = better)
- **Calibration**: Do markets priced at X% resolve YES X% of the time?
- **Mispricing Signals**: Illiquid extremes, large price moves
- **Slow Reactions**: Markets with consistent directional drift
- **Bias Detection**: Systematic over/under-confidence by source, region, or season
