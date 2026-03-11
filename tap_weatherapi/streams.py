"""Stream type classes for tap-weatherapi."""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, ClassVar

from singer_sdk import typing as th
from singer_sdk.pagination import BaseAPIPaginator, SinglePagePaginator

from tap_weatherapi.client import WeatherAPIStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Context, Record


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


class DateRangePaginator(BaseAPIPaginator[date]):
    """Paginator that advances through consecutive 30-day windows."""

    @override
    def __init__(self, start_date: date, end_date: date) -> None:
        """Initialize the paginator."""
        self._end_date = end_date
        super().__init__(start_value=start_date)
        if start_date > end_date:
            # Nothing to fetch - mark done before any request is made.
            self._finished = True

    @override
    def get_next(self, response: requests.Response) -> date | None:
        next_start = self._value + timedelta(days=30)
        return next_start if next_start <= self._end_date else None


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
    @property
    def partitions(self) -> list[dict[str, Any]]:
        return [{"location": loc} for loc in self.config["locations"]]

    @override
    def get_new_paginator(self) -> SinglePagePaginator:
        return SinglePagePaginator()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        location = context["location"] if context else self.config["locations"][0]
        return {
            "q": location,
            "days": self.config.get("forecast_days", 5),
            "aqi": "no",
            "alerts": "no",
        }


class HistoricalStream(WeatherAPIStream[date]):
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
    @property
    def partitions(self) -> list[dict[str, Any]]:
        return [{"location": loc} for loc in self.config["locations"]]

    # ------------------------------------------------------------------
    # Store context before paginator is created (SDK doesn't pass it to
    # get_new_paginator, so we cache it here for one sync pass at a time).
    # ------------------------------------------------------------------

    @override
    def request_records(self, context: Context | None) -> Iterable[Record]:
        self._sync_context: Context | None = context
        yield from super().request_records(context)

    @override
    def get_new_paginator(self) -> DateRangePaginator:
        context = getattr(self, "_sync_context", None)
        start = self._effective_start_date(context)
        end = _today() - timedelta(days=1)
        return DateRangePaginator(start_date=start, end_date=end)

    def _effective_start_date(self, context: Context | None) -> date:
        """Return start date from state bookmark (incremental) or config."""
        state_val = self.get_starting_replication_key_value(context)
        if state_val:
            # Advance one day past the last synced date to avoid re-processing.
            raw = state_val if isinstance(state_val, str) else str(state_val)
            return date.fromisoformat(raw[:10]) + timedelta(days=1)
        return date.fromisoformat(self.config["start_date"])

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: date | None,
    ) -> dict[str, Any]:
        if next_page_token is None:
            # Shouldn't happen - DateRangePaginator always provides a start date.
            return {}
        window_end = min(next_page_token + timedelta(days=29), _today() - timedelta(days=1))
        location = context["location"] if context else self.config["locations"][0]
        return {
            "q": location,
            "dt": next_page_token.isoformat(),
            "end_dt": window_end.isoformat(),
        }
