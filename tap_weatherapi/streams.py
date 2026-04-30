"""Stream type classes for tap-weatherapi."""

from __future__ import annotations

import sys
from dataclasses import KW_ONLY, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, ClassVar

from singer_sdk import typing as th
from singer_sdk.pagination import BaseAPIPaginator, SinglePagePaginator

from tap_weatherapi.client import BulkChunk, WeatherAPIStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context


# ---------------------------------------------------------------------------
# Shared schema fragments
# ---------------------------------------------------------------------------

_CONDITION_SCHEMA = th.ObjectType(
    th.Property("text", th.StringType),
    th.Property("icon", th.StringType),
    th.Property("code", th.IntegerType),
)

_HOUR_SCHEMA = th.ArrayType(
    th.ObjectType(
        th.Property("time_epoch", th.IntegerType),
        th.Property("time", th.StringType),
        th.Property("temp_c", th.NumberType),
        th.Property("temp_f", th.NumberType),
        th.Property("is_day", th.IntegerType),
        th.Property("condition", _CONDITION_SCHEMA),
        th.Property("wind_mph", th.NumberType),
        th.Property("wind_kph", th.NumberType),
        th.Property("wind_degree", th.IntegerType),
        th.Property("wind_dir", th.StringType),
        th.Property("pressure_mb", th.NumberType),
        th.Property("pressure_in", th.NumberType),
        th.Property("precip_mm", th.NumberType),
        th.Property("precip_in", th.NumberType),
        th.Property("snow_cm", th.NumberType),
        th.Property("humidity", th.IntegerType),
        th.Property("cloud", th.IntegerType),
        th.Property("feelslike_c", th.NumberType),
        th.Property("feelslike_f", th.NumberType),
        th.Property("windchill_c", th.NumberType),
        th.Property("windchill_f", th.NumberType),
        th.Property("heatindex_c", th.NumberType),
        th.Property("heatindex_f", th.NumberType),
        th.Property("dewpoint_c", th.NumberType),
        th.Property("dewpoint_f", th.NumberType),
        th.Property("will_it_rain", th.IntegerType),
        th.Property("chance_of_rain", th.IntegerType),
        th.Property("will_it_snow", th.IntegerType),
        th.Property("chance_of_snow", th.IntegerType),
        th.Property("vis_km", th.NumberType),
        th.Property("vis_miles", th.NumberType),
        th.Property("gust_mph", th.NumberType),
        th.Property("gust_kph", th.NumberType),
        th.Property("uv", th.NumberType),
    )
)

# One record per day - shared by both forecast and historical streams.
_WEATHER_DAY_SCHEMA = th.PropertiesList(
    # Partition / primary key fields
    th.Property(
        "location",
        th.StringType,
        description="The query string used (zip, city, lat/lon, …)",
    ),
    th.Property(
        "custom_id",
        th.StringType,
        description="A custom identifier for the location",
    ),
    th.Property("date", th.StringType, description="Calendar date (YYYY-MM-DD)"),
    th.Property("date_epoch", th.IntegerType),
    # Resolved location
    th.Property("location_name", th.StringType),
    th.Property("location_region", th.StringType),
    th.Property("location_country", th.StringType),
    th.Property("location_lat", th.NumberType),
    th.Property("location_lon", th.NumberType),
    th.Property("location_tz_id", th.StringType),
    # Day aggregates
    th.Property("maxtemp_c", th.NumberType),
    th.Property("maxtemp_f", th.NumberType),
    th.Property("mintemp_c", th.NumberType),
    th.Property("mintemp_f", th.NumberType),
    th.Property("avgtemp_c", th.NumberType),
    th.Property("avgtemp_f", th.NumberType),
    th.Property("maxwind_mph", th.NumberType),
    th.Property("maxwind_kph", th.NumberType),
    th.Property("totalprecip_mm", th.NumberType),
    th.Property("totalprecip_in", th.NumberType),
    th.Property("totalsnow_cm", th.NumberType),
    th.Property("avgvis_km", th.NumberType),
    th.Property("avgvis_miles", th.NumberType),
    th.Property("avghumidity", th.NumberType),
    th.Property("daily_will_it_rain", th.IntegerType),
    th.Property("daily_chance_of_rain", th.IntegerType),
    th.Property("daily_will_it_snow", th.IntegerType),
    th.Property("daily_chance_of_snow", th.IntegerType),
    th.Property("condition", _CONDITION_SCHEMA),
    th.Property("uv", th.NumberType),
    # Astronomy
    th.Property("sunrise", th.StringType),
    th.Property("sunset", th.StringType),
    th.Property("moonrise", th.StringType),
    th.Property("moonset", th.StringType),
    th.Property("moon_phase", th.StringType),
    th.Property("moon_illumination", th.IntegerType),
    # Hourly breakdown (nested)
    th.Property("hour", _HOUR_SCHEMA),
)


# ---------------------------------------------------------------------------
# Paginator for 30-day historical windows
# ---------------------------------------------------------------------------


@dataclass(slots=True, eq=True)
class DateWindow:
    """A closed date interval [start, end] representing one paginator window."""

    _: KW_ONLY

    start: date
    end: date

    def __repr__(self) -> str:
        """Return a string representation of the date window."""
        return f"DateWindow({self.start} → {self.end})"


class DateRangePaginator(BaseAPIPaginator[DateWindow]):
    """Paginator that advances through consecutive fixed-size date windows.

    Each page token is a :class:`DateWindow` carrying both the start and end
    of the window, so ``get_url_params`` can consume them directly without
    needing to know the window size or the global end date ceiling.
    """

    @override
    def __init__(self, start_date: date, end_date: date, window_size: int = 30) -> None:
        """Initialize the paginator."""
        self._end_date = end_date
        self._window_size = window_size
        first = DateWindow(
            start=start_date,
            end=min(start_date + timedelta(days=window_size - 1), end_date),
        )
        super().__init__(start_value=first)
        if start_date > end_date:
            # Nothing to fetch - mark done before any request is made.
            self._finished = True

    @override
    def get_next(self, response: requests.Response) -> DateWindow | None:
        next_start = self._value.end + timedelta(days=1)
        if next_start > self._end_date:
            return None
        next_end = min(next_start + timedelta(days=self._window_size - 1), self._end_date)
        return DateWindow(start=next_start, end=next_end)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _today() -> date:
    return datetime.now(timezone.utc).date()


# ---------------------------------------------------------------------------
# Streams
# ---------------------------------------------------------------------------


class ForecastStream(WeatherAPIStream[Any]):
    """Five-day weather forecast, one record per day per location."""

    name = "forecast"
    path = "/forecast.json"
    primary_keys: ClassVar[tuple[str, ...]] = ("location", "date")
    replication_key = None
    schema = _WEATHER_DAY_SCHEMA.to_dict()

    @override
    def get_non_bulk_paginator(self) -> SinglePagePaginator:
        return SinglePagePaginator()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: None | BulkChunk[None],
    ) -> dict[str, Any]:
        if isinstance(next_page_token, BulkChunk):
            query = "bulk"
        else:
            assert context is not None  # noqa: S101
            query = context["location"]

        return {
            "q": query,
            "days": self.config.get("forecast_days", 5),
            "aqi": "no",
            "alerts": "no",
        }


class HistoricalStream(WeatherAPIStream[DateWindow]):
    """Daily historical weather, one record per day per location.

    Fetches from ``start_date`` config (or last bookmark) to yesterday,
    paging through 30-day windows as required by the WeatherAPI history
    endpoint.
    """

    name = "historical"
    path = "/history.json"
    primary_keys: ClassVar[tuple[str, ...]] = ("location", "date")
    replication_key = "date"
    schema = _WEATHER_DAY_SCHEMA.to_dict()

    @override
    def get_non_bulk_paginator(self) -> DateRangePaginator:
        start = self._effective_start_date(self.context)
        end = _today() - timedelta(days=1)
        return DateRangePaginator(start_date=start, end_date=end)

    def _effective_start_date(self, context: Context | None) -> date:
        """Return start date from state bookmark (incremental) or config."""
        state_val = self.get_starting_replication_key_value(context)
        if state_val:
            # Advance one day past the last synced date to avoid re-processing.
            raw = state_val if isinstance(state_val, str) else str(state_val)
            return datetime.fromisoformat(raw).date()
        return datetime.fromisoformat(self.config["start_date"]).date()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: DateWindow | BulkChunk[DateWindow] | None,
    ) -> dict[str, Any]:
        assert next_page_token is not None  # noqa: S101

        if isinstance(next_page_token, BulkChunk):
            query = "bulk"
            window: DateWindow = next_page_token.current_value  # ty:ignore[invalid-assignment]
        else:
            assert context is not None  # noqa: S101
            query = context["location"]
            window = next_page_token

        return {
            "q": query,
            "dt": window.start.isoformat(),
            "end_dt": window.end.isoformat(),
        }
