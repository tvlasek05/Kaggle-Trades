# Weather Prediction Market & Weather Data API Research

## Part 1: Prediction Market Platforms with Weather Markets

---

### 1. Kalshi (Best for Weather — Most Structured)

**Status:** Fully operational, CFTC-regulated US exchange
**Weather Coverage:** Daily high/low temperatures, precipitation, hurricanes, monthly temperature ranges

#### Public API (No Authentication Required for Market Data)

**Base URL:** `https://api.elections.kalshi.com/trade-api/v2`
(Despite the "elections" subdomain, this serves ALL Kalshi markets including weather.)

**Key Endpoints (all public, no auth):**

```
# Get all open markets for a weather series
GET /trade-api/v2/markets?series_ticker=KXHIGHNY&status=open

# Get a specific series definition
GET /trade-api/v2/series/KXHIGHNY

# Get all available series
GET /trade-api/v2/series

# Get a specific market by ticker
GET /trade-api/v2/markets/{ticker}

# Get orderbook for a market
GET /trade-api/v2/markets/{ticker}/orderbook
```

**Weather Series Tickers:**

| Series Ticker | Description |
|---|---|
| `KXHIGHNY` | Highest temperature in NYC (Central Park) |
| `KXHIGHCHI` | Highest temperature in Chicago (Midway Airport) |
| `KXHIGHMIA` | Highest temperature in Miami (MIA Airport) |
| `KXHIGHLAX` | Highest temperature in Los Angeles |
| `KXHIGHDEN` | Highest temperature in Denver |
| `KXHMONTHRANGE` | Monthly temperature increase/decrease |

Market tickers follow the pattern: `KXHIGHNY-26MAR22-B55` (series-date-bracket).

**Example Full URL:**
```
https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker=KXHIGHNY&status=open&limit=10
```

**Response Structure:**
```json
{
  "markets": [
    {
      "ticker": "KXHIGHNY-26MAR22-B55",
      "title": "NYC High Temp: 55°F or above?",
      "event_ticker": "KXHIGHNY-26MAR22",
      "series_ticker": "KXHIGHNY",
      "category": "Climate and Weather",
      "status": "open",
      "yes_bid_dollars": "0.65",
      "no_bid_dollars": "0.35",
      "volume_fp": "12345",
      "open_time": "2026-03-21T15:00:00Z",
      "close_time": "2026-03-22T23:59:00Z",
      "settlement_timer_seconds": 86400
    }
  ],
  "cursor": "..."
}
```

**Market Structure:** Each day's temperature event has ~6 brackets. The middle 4 brackets are 2°F wide; the 2 edge brackets cover everything above/below.

**Settlement:** Based on the NWS Daily Climate Report issued the following morning.

**Pagination:** Use `limit` (1-1000, default 100) and `cursor` parameters.

**Rate Limits:** Not explicitly documented for public endpoints, but reasonable use is expected.

**Docs:** https://docs.kalshi.com/getting_started/quick_start_market_data

---

### 2. Polymarket (Largest by Volume)

**Status:** Fully operational, largest prediction market by volume ($44B+ in 2025)
**Weather Coverage:** ~489 active weather markets (storms, hurricanes, temperature records, climate events)

#### Public API (No Authentication Required for Read)

Polymarket has three separate APIs:

| API | Base URL | Auth Required? |
|---|---|---|
| **Gamma API** (markets/metadata) | `https://gamma-api.polymarket.com` | No |
| **Data API** (positions/trades) | `https://data-api.polymarket.com` | No |
| **CLOB API** (orderbook/pricing) | `https://clob.polymarket.com` | No for reads, Yes for trading |

**Key Gamma API Endpoints (all public):**

```
# List markets with tag filter
GET /markets?tag=weather&limit=25&offset=0&closed=false

# Get a specific market by slug
GET /markets?slug={market-slug}

# List events
GET /events?tag=weather&limit=25

# Search markets
GET /markets?_q=temperature
```

**Key CLOB API Endpoints (public for reads):**

```
# Get orderbook for a token
GET /book?token_id={token_id}

# Get midpoint price
GET /midpoint?token_id={token_id}

# Get price history
GET /prices-history?market={condition_id}&interval=1d

# Get last trade price
GET /last-trade-price?token_id={token_id}

# Get spread
GET /spread?token_id={token_id}
```

**Example Full URL:**
```
https://gamma-api.polymarket.com/markets?tag=weather&limit=5&closed=false&order=volume&ascending=false
```

**Response Structure (Gamma /markets):**
```json
[
  {
    "id": "12345",
    "question": "Will NYC reach 90°F before June 1?",
    "conditionId": "0xabc...",
    "slug": "will-nyc-reach-90f-before-june-1",
    "category": "Weather",
    "endDate": "2026-06-01T00:00:00Z",
    "outcomes": "[\"Yes\",\"No\"]",
    "outcomePrices": "[\"0.25\",\"0.75\"]",
    "volume": "150000",
    "liquidity": "25000",
    "active": true,
    "closed": false,
    "description": "This market resolves Yes if...",
    "resolutionSource": "NOAA",
    "startDate": "2026-03-01T00:00:00Z",
    "image": "https://...",
    "marketType": "binary"
  }
]
```

**Rate Limits:** 60 requests/minute for REST; WebSocket connections have minimal restrictions.

**Weather Market Types on Polymarket:**
- Daily high temperatures for specific cities
- Hurricane/tropical storm formation and landfall
- Seasonal temperature records
- Solar storm events
- Named storm counts

**Docs:** https://docs.polymarket.com/developers/gamma-markets-api/overview

---

### 3. Metaculus (Forecasting Platform — Not a Trading Market)

**Status:** Operational, free public forecasting platform
**Weather Coverage:** Climate and weather forecasting questions (fewer, more long-term)

#### Public API

**Base URL:** `https://www.metaculus.com/api2`

**Key Endpoints:**

```
# Search for weather questions
GET /api2/questions/?search=weather&status=open&limit=20&offset=0

# Get a specific question
GET /api2/questions/{question_id}/

# List questions by type
GET /api2/questions/?forecast_type=binary&search=temperature
```

**Example Full URL:**
```
https://www.metaculus.com/api2/questions/?search=weather+temperature&status=open&limit=10
```

**Response Structure:**
```json
{
  "count": 42,
  "next": "https://www.metaculus.com/api2/questions/?limit=10&offset=10&search=weather",
  "previous": null,
  "results": [
    {
      "id": 15536,
      "title": "6-Month AI Weather Forecasting?",
      "type": "binary",
      "status": "open",
      "created_at": "2023-06-15T...",
      "scheduled_resolve_time": "2026-12-31T...",
      "resolution": null,
      "aggregations": {
        "metaculus_prediction": {
          "latest": {
            "centers": [0.35]
          }
        },
        "recency_weighted": {
          "latest": {
            "centers": [0.40]
          }
        }
      },
      "forecasts_count": 150,
      "description": "..."
    }
  ]
}
```

**Auth:** API token required for submitting forecasts (get one at https://metaculus.com/aib). Reading public questions may work without auth.

**Note:** Metaculus is a forecasting/reputation platform, not a financial trading market. Questions tend to be longer-term and more about climate trends than daily weather.

**Docs:** https://www.metaculus.com/api/ and https://www.metaculus.com/api2/schema/redoc/

---

### 4. ForecastEx (via Interactive Brokers)

**Status:** Operational since November 2025
**Weather Coverage:** Daily high-temperature markets

ForecastEx offers daily high-temperature markets through Interactive Brokers' ForecastTrader platform. However, API access is through the Interactive Brokers API (TWS or IB Gateway), which requires an IB account. This is **not a free public API**.

---

### 5. Robinhood Prediction Markets

**Status:** Operational
**Weather Coverage:** Rain and snow forecasts across US states

Robinhood offers weather prediction contracts but does **not** provide a public API for market data. Access is through the Robinhood app only.

---

## Part 2: Free Weather Data APIs (for Verification/Outcomes)

---

### 1. Open-Meteo (Best Free Option — No Key Required)

**Status:** Fully operational, open-source
**Cost:** Free for non-commercial use (<10,000 calls/day). No API key needed.

#### Forecast API

```
GET https://api.open-meteo.com/v1/forecast?latitude=40.78&longitude=-73.97&hourly=temperature_2m&temperature_unit=fahrenheit&forecast_days=7
```

**Response:**
```json
{
  "latitude": 40.78,
  "longitude": -73.97,
  "elevation": 51.0,
  "generationtime_ms": 0.5,
  "utc_offset_seconds": 0,
  "timezone": "GMT",
  "timezone_abbreviation": "GMT",
  "hourly": {
    "time": ["2026-03-22T00:00", "2026-03-22T01:00", "..."],
    "temperature_2m": [42.1, 41.5, 40.8, "..."]
  },
  "hourly_units": {
    "temperature_2m": "°F"
  }
}
```

#### Historical Weather API (for actual past observations)

```
GET https://archive-api.open-meteo.com/v1/archive?latitude=40.78&longitude=-73.97&start_date=2026-03-20&end_date=2026-03-20&hourly=temperature_2m&temperature_unit=fahrenheit
```

**Response:** Same structure as forecast, but with actual observed/reanalysis data.

#### Key Parameters

| Parameter | Description |
|---|---|
| `latitude`, `longitude` | Location coordinates |
| `hourly` | Variables: `temperature_2m`, `precipitation`, `windspeed_10m`, `relative_humidity_2m`, etc. |
| `daily` | Variables: `temperature_2m_max`, `temperature_2m_min`, `precipitation_sum`, etc. |
| `temperature_unit` | `celsius` (default) or `fahrenheit` |
| `forecast_days` | 1-16 days |
| `start_date`, `end_date` | For historical data (YYYY-MM-DD) |
| `timezone` | e.g., `America/New_York` |

**Useful Coordinates for Kalshi Markets:**
- NYC Central Park: `latitude=40.7829&longitude=-73.9654`
- Chicago Midway: `latitude=41.7868&longitude=-87.7522`
- Miami Airport: `latitude=25.7959&longitude=-80.2870`
- Los Angeles: `latitude=34.0522&longitude=-118.2437`
- Denver: `latitude=39.7392&longitude=-104.9903`

**Docs:** https://open-meteo.com/en/docs

---

### 2. NWS API (api.weather.gov) — US Government, Completely Free

**Status:** Fully operational, official US government service
**Cost:** Completely free, no API key needed. Just set a `User-Agent` header.

#### Key Endpoints

```
# Get latest observation from a station
GET https://api.weather.gov/stations/{stationId}/observations/latest

# Get forecast for a grid point
GET https://api.weather.gov/gridpoints/{office}/{gridX},{gridY}/forecast

# Get point metadata (to find grid coordinates)
GET https://api.weather.gov/points/{lat},{lon}

# Get active weather alerts
GET https://api.weather.gov/alerts/active?area={state}
```

**Relevant Station IDs for Kalshi Markets:**
- NYC Central Park: `KNYC` (or nearby `KLGA`, `KJFK`)
- Chicago Midway: `KMDW`
- Miami International: `KMIA`
- Los Angeles: `KLAX`
- Denver: `KDEN`

**Example — Latest Observation:**
```
GET https://api.weather.gov/stations/KNYC/observations/latest
Header: User-Agent: WeatherApp (contact@example.com)
```

**Response (GeoJSON):**
```json
{
  "type": "Feature",
  "properties": {
    "station": "https://api.weather.gov/stations/KNYC",
    "timestamp": "2026-03-22T14:00:00+00:00",
    "temperature": {
      "value": 8.3,
      "unitCode": "wmoUnit:degC",
      "qualityControl": "V"
    },
    "windSpeed": {
      "value": 12.5,
      "unitCode": "wmoUnit:km_h-1"
    },
    "barometricPressure": {
      "value": 101500,
      "unitCode": "wmoUnit:Pa"
    },
    "relativeHumidity": {
      "value": 65.2,
      "unitCode": "wmoUnit:percent"
    },
    "textDescription": "Partly Cloudy"
  }
}
```

**Important:** Temperature is returned in Celsius by default. Convert with: `°F = (°C × 9/5) + 32`

**Rate Limits:** Reasonable use expected; no hard published limit. Set a descriptive `User-Agent` header.

**Docs:** https://www.weather.gov/documentation/services-web-api

---

### 3. NOAA Climate Data Online (CDO) API — Free with Token

**Status:** Operational
**Cost:** Free, but requires a free API token (sign up at https://www.ncdc.noaa.gov/cdo-web/token)

**Base URL:** `https://www.ncei.noaa.gov/cdo-web/api/v2`

**Key Endpoints:**
```
# Daily summaries (GHCND dataset)
GET /data?datasetid=GHCND&locationid=ZIP:10001&startdate=2026-03-20&enddate=2026-03-20&datatypeid=TMAX,TMIN&units=standard

# List available stations
GET /stations?locationid=FIPS:36&datasetid=GHCND&limit=25
```

**Headers Required:**
```
token: {your-api-token}
```

**Rate Limits:** 5 requests/second, 10,000 requests/day

**Best For:** Historical climate data going back decades. GHCND dataset has daily max/min temperatures, precipitation, snowfall.

**Docs:** https://www.ncdc.noaa.gov/cdo-web/webservices/v2

---

## Part 3: Summary — Recommended Stack

### For a Weather Prediction Market Trading/Analysis System:

| Purpose | Best API | Auth? | Cost |
|---|---|---|---|
| **Weather market prices** | Kalshi API | No (reads) | Free |
| **Weather market prices (alt)** | Polymarket Gamma API | No | Free |
| **Weather forecasts** | Open-Meteo Forecast API | No | Free |
| **Actual weather outcomes** | NWS API (api.weather.gov) | No (just User-Agent) | Free |
| **Historical weather** | Open-Meteo Archive API | No | Free |
| **Historical climate data** | NOAA CDO API | Free token | Free |

### Recommended Verification Workflow:

1. **Fetch Kalshi market prices** for a city's daily high temperature
2. **Fetch Open-Meteo forecast** for the same location to compare market odds vs. weather models
3. **After the day passes**, fetch the NWS observation from `api.weather.gov` to determine the actual outcome
4. **Compare** market-implied probability vs. forecast vs. actual outcome

### Key Insight for Kalshi Weather Markets:

Kalshi settles based on the **NWS Daily Climate Report**, so `api.weather.gov` is the authoritative source for resolution. The station observations endpoint gives you the same data the markets settle against.
