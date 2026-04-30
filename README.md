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
| `api_key` | ✅ | - | WeatherAPI key — get one at https://www.weatherapi.com/my/ |
| `locations` | ✅\* | - | JSON array of locations to sync. Accepts city names, US zip codes, UK/Canada postcodes, `lat,lon` pairs, airport codes, IP addresses, and [more](https://www.weatherapi.com/docs/#intro-request). |
| `locations_file` | ✅\* | - | Path to a JSON file listing locations. Format: `[{"location": "90210", "custom_id": "beverly-hills"}, ...]`. Use instead of `locations` when managing many locations. |
| `start_date` | ✅ | - | Earliest date for the `historical` stream (`YYYY-MM-DD`). |
| `end_date` | - | Yesterday's date | Latest date for the `historical` stream (`YYYY-MM-DD`). |
| `forecast_days` | - | `5` | Number of days for the `forecast` stream (1–14). |
| `use_bulk_requests` | - | `false` | Fetch all locations in a single POST request. Requires Pro+, Business, or Enterprise plan. Max 50 locations. See [Bulk API](#bulk-api). |

\* Exactly one of `locations` or `locations_file` is required.

Example `config.json` (inline locations):

```json
{
  "api_key": "YOUR_API_KEY",
  "locations": ["10001", "90210", "60605"],
  "start_date": "2024-01-01"
}
```

Example `config.json` (locations file):

```json
{
  "api_key": "YOUR_API_KEY",
  "locations_file": "/path/to/locations.json",
  "start_date": "2024-01-01"
}
```

`locations.json`:

```json
[
  {"location": "10001", "custom_id": "new-york"},
  {"location": "90210", "custom_id": "beverly-hills"},
  {"location": "60605", "custom_id": "chicago"}
]
```

or as a JSON lines file with a `.jsonl` file extension:

```json
{"location": "10001", "custom_id": "new-york"}
{"location": "90210", "custom_id": "beverly-hills"}
{"location": "60605", "custom_id": "chicago"}
```

### Bulk API

When `use_bulk_requests` is `true`, the tap sends all locations in a single POST request to the WeatherAPI [Bulk endpoint](https://www.weatherapi.com/docs/#intro-bulk) instead of making one GET request per location.

Requirements and limits:

- Requires a **WeatherAPI Pro+, Business, or Enterprise** plan.
- The API accepts a maximum of **50 locations per request**. When your list exceeds 50, the tap automatically splits it into consecutive chunks of 50 and makes one request per chunk.
- Each location still counts as one API call toward your quota regardless of chunking.
- `custom_id` in `locations_file` is passed through the bulk request and back into each emitted record, making it easy to join results to your own identifiers.
- **State:** In bulk mode the tap maintains a single shared state bookmark across all locations. If you add or remove locations between runs, re-run with `--full-refresh` to ensure all locations are synced from the beginning.

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
