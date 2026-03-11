"""REST client handling, including WeatherAPIStream base class."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Generic, TypeVar

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Context, Record

_T = TypeVar("_T")


class WeatherAPIStream(RESTStream[_T], Generic[_T]):
    """WeatherAPI stream class."""

    @override
    @property
    def url_base(self) -> str:
        return "https://api.weatherapi.com/v1"

    @override
    @property
    def authenticator(self) -> APIKeyAuthenticator:
        return APIKeyAuthenticator(
            key="key",
            value=self.config["api_key"],
            location="params",
        )

    @override
    def parse_response(self, response: requests.Response) -> Iterable[Record]:
        """Extract one record per forecast/history day from the WeatherAPI response."""
        data = response.json()
        loc = data.get("location", {})
        loc_info = {
            "location_name": loc.get("name"),
            "location_region": loc.get("region"),
            "location_country": loc.get("country"),
            "location_lat": loc.get("lat"),
            "location_lon": loc.get("lon"),
            "location_tz_id": loc.get("tz_id"),
        }
        for day in data.get("forecast", {}).get("forecastday", []):
            record: Record = {
                **loc_info,
                "date": day.get("date"),
                "date_epoch": day.get("date_epoch"),
                **day.get("day", {}),
            }
            astro = day.get("astro", {})
            record.update(
                {
                    "sunrise": astro.get("sunrise"),
                    "sunset": astro.get("sunset"),
                    "moonrise": astro.get("moonrise"),
                    "moonset": astro.get("moonset"),
                    "moon_phase": astro.get("moon_phase"),
                    "moon_illumination": astro.get("moon_illumination"),
                }
            )
            record["hour"] = day.get("hour", [])
            yield record

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        """Inject the query location string into each record."""
        if context:
            row["location"] = context.get("location")
        return row
