# Weather Prediction Market Tracker

Automated system for tracking, analyzing, and identifying opportunities in weather prediction markets.

## What it does

Each session:
1. **Pulls** latest weather prediction market data from Polymarket and Kalshi APIs
2. **Appends** new price snapshots to a persistent rolling time series (CSV)
3. **Fetches** actual weather outcomes from Open-Meteo (free, no API key)
4. **Computes** calibration, forecast error (Brier score), and implied vs actual probability
5. **Identifies** mispriced markets, arbitrage opportunities, slow reactions, and regional/seasonal biases
6. **Saves** updated datasets and analysis report to the repository
7. **Outputs** a concise summary of insights and trading opportunities

## Quick start

```bash
pip install -r requirements.txt
python run_session.py
```

## Project structure

```
├── run_session.py        # Main entry point - runs a full session
├── market_fetcher.py     # Polymarket + Kalshi API data collection
├── weather_actuals.py    # Open-Meteo weather verification data
├── analysis.py           # Calibration, forecast error, mispricing, bias detection
├── seed_data.py          # Generates realistic seed data for offline use
├── data/
│   ├── market_prices.csv      # Rolling time series of market prices
│   ├── markets_metadata.json  # Market descriptions and metadata
│   └── weather_actuals.csv    # Actual weather observations
└── analysis/
    └── latest_report.json     # Most recent analysis report
```

## Data sources

| Source | Type | Auth Required |
|--------|------|---------------|
| [Polymarket](https://polymarket.com) | Prediction markets | No (public API) |
| [Kalshi](https://kalshi.com) | Regulated prediction exchange | No (public read) |
| [Open-Meteo](https://open-meteo.com) | Weather actuals | No (free API) |

## Analysis outputs

- **Calibration table**: Implied probability vs actual resolution rate by bucket
- **Forecast errors**: Brier score and log loss per resolved market
- **Mispricing signals**: Arbitrage (Yes+No != 1.0), high volatility, thin extreme-priced markets
- **Bias detection**: Regional over/under-prediction, source-level calibration differences
- **Trading opportunities**: Ranked by signal strength
