"""WeatherAPI tap class."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_weatherapi import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

try:
    import requests_cache

    requests_cache.install_cache(allowable_methods=["GET", "POST"])
except ImportError:
    pass

if TYPE_CHECKING:
    from tap_weatherapi.client import WeatherAPIStream


class TapWeatherAPI(Tap):
    """Singer tap for WeatherAPI."""

    name = "tap-weatherapi"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="API Key",
            description="WeatherAPI key (https://www.weatherapi.com/my/)",
        ),
        th.Property(
            "locations",
            th.ArrayType(th.StringType(nullable=False)),
            required=False,
            title="Locations",
            description=(
                "One or more locations to fetch weather data for. "
                "Accepts city names, US zip codes, UK/Canada postcodes, lat/lon pairs, "
                "airport codes, IP addresses, and Search API IDs. "
                "See https://www.weatherapi.com/docs/#intro-request for full syntax. "
                "Use `locations_file` instead when syncing many locations."
            ),
        ),
        th.Property(
            "locations_file",
            th.StringType,
            required=False,
            title="Locations File",
            description=(
                "Path to a JSON file listing locations to sync. "
                'Format: [{"location": "90210", "custom_id": "beverly-hills"}, ...]. '
                "`location` accepts the same values as the `locations` setting. "
                "`custom_id` is optional; when provided it is passed through to each record "
                "and used to correlate bulk-request responses with the original query."
            ),
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=False),
            required=True,
            title="Start Date",
            description="Earliest date for historical data sync, in ISO format",
        ),
        th.Property(
            "end_date",
            th.DateTimeType(nullable=True),
            required=False,
            title="End Date",
            description=(
                "Latest date for historical data sync, in ISO format. Defaults to yesterday's date."
            ),
        ),
        th.Property(
            "forecast_days",
            th.IntegerType(nullable=False, minimum=1, maximum=14),
            default=5,
            title="Forecast Days",
            description="Number of days to include in the forecast stream (1-14).",
        ),
        th.Property(
            "use_bulk_requests",
            th.BooleanType(nullable=False),
            default=False,
            title="Use Bulk Requests",
            description=(
                "Send all locations in a single POST request instead of one GET request per "
                "location. "
                "Requires a WeatherAPI Pro+, Business, or Enterprise plan. "
                "Maximum 50 locations per request. "
                "Each location in a bulk request still counts as one API call. "
                "See https://www.weatherapi.com/docs/#intro-bulk."
            ),
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[WeatherAPIStream[Any]]:
        return [
            streams.ForecastStream(self),
            streams.HistoricalStream(self),
        ]


if __name__ == "__main__":
    TapWeatherAPI.cli()
