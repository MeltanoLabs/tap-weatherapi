"""REST client handling, including WeatherAPIStream base class."""

from __future__ import annotations

import json
import logging
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.pagination import BaseAPIPaginator
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
_BULK_CHUNK_SIZE = 50  # WeatherAPI bulk endpoint limit

logger = logging.getLogger(__name__)


def _extract_records(response: dict[str, Any]) -> Iterable[Record]:
    loc = response.get("location", {})
    loc_info = {
        "location_name": loc.get("name"),
        "location_region": loc.get("region"),
        "location_country": loc.get("country"),
        "location_lat": loc.get("lat"),
        "location_lon": loc.get("lon"),
        "location_tz_id": loc.get("tz_id"),
    }
    for day in response.get("forecast", {}).get("forecastday", []):
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


@cache
def _get_location_from_file(path: Path) -> list[dict[str, Any]]:
    logger.info("Loading locations from file: %s", path)

    with open(path) as f:  # noqa: PTH123
        data = [json.loads(line) for line in f] if path.suffix == ".jsonl" else json.load(f)
        if (
            not isinstance(data, list)  # Expecting a list of location objects
            or not all(isinstance(loc, dict) and "location" in loc for loc in data)
        ):
            logger.error(
                "Expected a list of location objects in 'locations_file', got %s",
                data,
            )
            msg = "Invalid format in locations_file"
            raise ValueError(msg)
        return data


@dataclass
class BulkChunk(Generic[_T]):
    """A chunk of paginated data."""

    data: list[dict[str, Any]]
    """A chunk of the data."""

    current_value: _T
    """The actual pagination value."""


class BulkChunkPaginationWrapper(BaseAPIPaginator[BulkChunk[_T]], Generic[_T]):
    """A paginator that wraps another paginator to request data in chunks."""

    def __init__(
        self,
        *,
        wrapped: BaseAPIPaginator[_T],
        chunks: list[list[dict[str, Any]]],
    ) -> None:
        """Initialize chunked paginator."""
        self._current_index = 0
        self._chunks = chunks
        self._wrapped = wrapped
        self._initial_value = self._wrapped.current_value

        super().__init__(
            start_value=BulkChunk(
                data=self._chunks[self._current_index],
                current_value=self._initial_value,
            )
        )

    @override
    def get_next(self, response: requests.Response) -> BulkChunk[_T] | None:
        next_inner = self._wrapped.get_next(response)

        if self._wrapped.has_more(response) and next_inner is not None:
            # More pages remain in the current chunk; advance the inner paginator.
            self._wrapped._value = next_inner  # noqa: SLF001
            return BulkChunk(data=self._chunks[self._current_index], current_value=next_inner)

        # Inner paginator exhausted — move to the next chunk if one exists.
        if self._current_index < len(self._chunks) - 1:
            self._current_index += 1
            self._wrapped._value = self._initial_value  # noqa: SLF001
            return BulkChunk(
                data=self._chunks[self._current_index],
                current_value=self._initial_value,
            )

        return None


class WeatherAPIStream(RESTStream[_T], ABC, Generic[_T]):
    """WeatherAPI stream class."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the stream, setting the HTTP method based on config."""
        super().__init__(*args, **kwargs)
        # Bulk: [] = single global bookmark shared across all locations.
        # Non-bulk: key state on "location" only so changing custom_id in
        # locations_file doesn't reset incremental bookmarks.
        if self.config["use_bulk_requests"]:
            self.logger.info("Using 'bulk' requests")
            self._http_method = "POST"
            self.state_partitioning_keys = None
        else:
            self._http_method = "GET"
            self.state_partitioning_keys = ["location"]

        if locations := self.config.get("locations"):
            self.logger.info("Using locations from config")
            self._locations = [{"location": loc} for loc in locations]
        elif locations_file := self.config.get("locations_file"):
            self._locations = _get_location_from_file(Path(locations_file))
        else:
            msg = "Either 'locations' or 'locations_file' config must be provided"
            raise ValueError(msg)

    @property
    @override
    def url_base(self) -> str:
        return "https://api.weatherapi.com/v1"

    @property
    @override
    def authenticator(self) -> APIKeyAuthenticator:
        return APIKeyAuthenticator(
            key="key",
            value=self.config["api_key"],
            location="params",
        )

    @property
    @override
    def partitions(self) -> list[dict[str, Any]]:
        if self.config["use_bulk_requests"]:
            # Single empty partition; chunk iteration is handled inside get_records.
            return [{}]
        return self._locations

    @abstractmethod
    def get_non_bulk_paginator(self) -> BaseAPIPaginator[_T]:
        """Get a simple paginator instance for this stream."""

    @override
    def get_new_paginator(self) -> BaseAPIPaginator[_T] | BulkChunkPaginationWrapper[_T]:
        inner = self.get_non_bulk_paginator()
        if not self.config["use_bulk_requests"]:
            return inner

        chunks = [
            self._locations[i : i + _BULK_CHUNK_SIZE]
            for i in range(0, len(self._locations), _BULK_CHUNK_SIZE)
        ]
        return BulkChunkPaginationWrapper(wrapped=inner, chunks=chunks)

    @override
    def parse_response(self, response: requests.Response) -> Iterable[Record]:
        """Extract one record per forecast/history day from the WeatherAPI response."""
        data = response.json()
        if self.config["use_bulk_requests"]:
            for entry in data["bulk"]:
                for record in _extract_records(entry["query"]):
                    record["custom_id"] = entry["query"].get("custom_id")
                    record["location"] = entry["query"].get("q")
                    yield record
        else:
            yield from _extract_records(data)

    @override
    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: _T | BulkChunk[_T] | None,
    ) -> dict[str, Any] | None:
        if isinstance(next_page_token, BulkChunk):
            return {
                "locations": [
                    {
                        "q": loc["location"],
                        "custom_id": loc.get("custom_id"),
                    }
                    for loc in next_page_token.data
                ]
            }

        return None

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        """Inject the query location string into each record."""
        if self.config["use_bulk_requests"]:
            # location and custom_id are already set per-entry in parse_response.
            return row

        row["custom_id"] = context.get("custom_id") if context else None
        row["location"] = context.get("location") if context else None
        return row
