# Weather Prediction Market Analysis

## Scheduled Pipeline

When running as a scheduled session, execute the full pipeline:

```bash
pip install -r requirements.txt
python main.py --commit
```

Then push the committed data:

```bash
git push -u origin main
```

After running, read `output/summary.txt` and provide a concise summary of:
- How many markets were fetched/updated
- Calibration highlights (any buckets with >10% error)
- Top mispriced markets
- Notable biases (regional or seasonal)
- Any trading opportunities identified

## Project Structure

- `main.py` — Entry point, orchestrates the pipeline
- `fetch_markets.py` — Pulls Kalshi weather prediction markets
- `fetch_polymarket.py` — Pulls Polymarket and Manifold weather markets
- `fetch_weather.py` — Fetches actual weather outcomes from Open-Meteo
- `analyze.py` — Calibration, mispricing, bias detection, slow reaction analysis
- `config.py` — API URLs, city coordinates, series mappings
- `data/` — Persistent CSV datasets (markets, prices, outcomes)
- `output/` — Analysis results and summaries
