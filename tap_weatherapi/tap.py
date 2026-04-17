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

    requests_cache.install_cache()
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
            th.ArrayType(th.StringType(nullable=False), nullable=False),
            required=True,
            title="Locations",
            description=(
                "One or more locations to fetch weather data for. "
                "Accepts US zip codes, city names, lat/lon pairs, etc. "
                "See https://www.weatherapi.com/docs/#intro-request for full syntax."
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
            "forecast_days",
            th.IntegerType(nullable=False),
            default=5,
            title="Forecast Days",
            description="Number of days to include in the forecast stream (1-14).",
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
