# tap-weatherapi

`tap-weatherapi` is a Singer tap for [WeatherAPI](https://www.weatherapi.com/).

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Streams

| Stream | Endpoint | Description |
|:-------|:---------|:------------|
| `forecast` | `/forecast.json` | Daily weather forecast for the next N days (default 5). One record per day per location. |
| `historical` | `/history.json` | Historical daily weather from `start_date` to yesterday. One record per day per location. Pages through 30-day windows automatically. Supports incremental sync. |

### Record structure

The WeatherAPI response is a nested object. This tap flattens it into **one record per day** with the following structure:

```
API response                       Emitted record field(s)
───────────────────────────────────────────────────────────────────────────────────────────────────────────
(config)                         → location                            ← the query string used
location.name/region/country/... → location_name, location_region, ...
forecastday[].date               → date, date_epoch
forecastday[].day.maxtemp_c/...  → maxtemp_c, mintemp_c, avgtemp_c, ...  (spread flat)
forecastday[].day.totalprecip_mm → totalprecip_mm, totalprecip_in, ...
forecastday[].day.condition      → condition                           ← kept as object {text, icon, code}
forecastday[].astro.*            → sunrise, sunset, moonrise, moon_phase, ...  (spread flat)
forecastday[].hour               → hour                                ← kept as array of 24 hourly objects
```

The `day` and `astro` sub-objects are spread into the top level of the record. The `hour` array is kept nested because it has 24 items each with ~30 fields - flattening it would produce 720 columns.

## Installation

Install from GitHub:

```bash
uv tool install git+https://github.com/MeltanoLabs/tap-weatherapi.git@main
```

## Configuration

### Accepted Config Options

| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| `api_key` | ✅ | - | WeatherAPI key - get one at https://www.weatherapi.com/my/ |
| `locations` | ✅ | - | JSON array of locations to sync. Accepts US zip codes, city names, `lat,lon` pairs, and [more](https://www.weatherapi.com/docs/#intro-request). |
| `start_date` | ✅ | - | Earliest date for the `historical` stream (`YYYY-MM-DD`). |
| `forecast_days` | - | `5` | Number of days for the `forecast` stream (1–14). |

Example `config.json`:

```json
{
  "api_key": "YOUR_API_KEY",
  "locations": ["10001", "90210", "60605"],
  "start_date": "2024-01-01",
  "forecast_days": 5
}
```

### Environment variables

Copy `.env.example` to `.env` and fill in your values. Pass `--config=ENV` to load them automatically:

```bash
cp .env.example .env
tap-weatherapi --config=ENV --discover
```

### Source Authentication and Authorization

A free WeatherAPI account provides access to both the forecast and history endpoints. Sign up at https://www.weatherapi.com/signup.aspx and copy your key from the [dashboard](https://www.weatherapi.com/my/).

## Usage

### Executing the tap directly

```bash
tap-weatherapi --version
tap-weatherapi --help
tap-weatherapi --config config.json --discover > catalog.json
tap-weatherapi --config config.json --catalog catalog.json
```

### Running with Meltano

```bash
# Install meltano
uv tool install meltano

# Test invocation
meltano invoke tap-weatherapi --version

# Run a full EL pipeline (tap → target-jsonl)
meltano run tap-weatherapi target-jsonl
```

## Developer Resources

### Set up the development environment

Prerequisites: Python 3.10+, [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Run the tests

```bash
uv run pytest
```

Set `TAP_WEATHERAPI_API_KEY` (and optionally the other `TAP_WEATHERAPI_*` vars) in your environment or `.env` file to run tests against the live API.

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more information on developing Singer taps with the Meltano SDK.
