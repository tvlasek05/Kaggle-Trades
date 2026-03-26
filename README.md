# Weather Prediction Market Analysis

Automated pipeline that tracks weather prediction markets (Kalshi), fetches actual weather outcomes (Open-Meteo), and analyzes calibration, mispricing, and biases.

## Setup

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Full pipeline: fetch markets -> fetch weather -> analyze
python main.py

# Individual steps
python main.py --fetch      # Pull latest market data
python main.py --weather    # Fetch actual weather outcomes
python main.py --analyze    # Run analysis only
```

## Data Files

- `data/markets.csv` — All tracked weather markets with latest prices
- `data/prices.csv` — Rolling time series of price snapshots
- `data/outcomes.csv` — Actual weather outcomes for resolved markets
- `output/analysis.csv` — Enriched dataset with implied probabilities and outcomes
- `output/summary.txt` — Human-readable analysis summary

## Pipeline

1. **Fetch Markets** — Pulls weather event contracts from Kalshi (temperature, snowfall, hurricanes) across NYC, Chicago, LA, Miami, Dallas, Denver, Boston
2. **Fetch Weather** — Gets actual weather observations from Open-Meteo for resolved market dates
3. **Analyze** — Computes calibration by probability bucket, identifies mispriced markets, detects regional/seasonal biases, flags slow market reactions, and surfaces trading opportunities
