"""Tests for stream-level behaviour (no real API calls required)."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError

from tap_weatherapi.streams import ForecastStream, HistoricalStream


def _make_stream(start_date: str = "2024-01-01") -> HistoricalStream:
    """Return a HistoricalStream instance backed by minimal mocked tap/config."""
    tap = MagicMock()
    tap.config = {
        "api_key": "test",
        "locations": ["London"],
        "start_date": start_date,
        "use_bulk_requests": False,
    }
    tap.state = {}
    tap.catalog = MagicMock()
    tap.catalog.get_stream.return_value = None
    return HistoricalStream(tap=tap)


class TestEffectiveStartDate:
    """Unit tests for HistoricalStream._effective_start_date."""

    def test_no_bookmark_returns_start_date(self) -> None:
        """With no state bookmark the config start_date is returned unchanged."""
        stream = _make_stream(start_date="2024-03-01")
        with patch.object(stream, "get_context_state", return_value={}):
            result = stream._effective_start_date(None)
        assert result == date(2024, 3, 1)

    def test_bookmark_returns_next_day(self) -> None:
        """With a state bookmark the day after the bookmark is returned."""
        stream = _make_stream(start_date="2024-01-01")
        state = {"replication_key_value": "2024-03-10"}
        with patch.object(stream, "get_context_state", return_value=state):
            result = stream._effective_start_date(None)
        assert result == date(2024, 3, 11)

    def test_bookmark_advances_across_month_boundary(self) -> None:
        """The +1 day offset crosses month boundaries correctly."""
        stream = _make_stream(start_date="2024-01-01")
        state = {"replication_key_value": "2024-01-31"}
        with patch.object(stream, "get_context_state", return_value=state):
            result = stream._effective_start_date(None)
        assert result == date(2024, 2, 1)

    def test_bookmark_advances_across_year_boundary(self) -> None:
        """The +1 day offset crosses year boundaries correctly."""
        stream = _make_stream(start_date="2024-01-01")
        state = {"replication_key_value": "2024-12-31"}
        with patch.object(stream, "get_context_state", return_value=state):
            result = stream._effective_start_date(None)
        assert result == date(2025, 1, 1)

    def test_bookmark_earlier_than_start_date_still_advances(self) -> None:
        """The bookmark value is used regardless of where start_date sits."""
        stream = _make_stream(start_date="2024-06-01")
        state = {"replication_key_value": "2024-01-15"}
        with patch.object(stream, "get_context_state", return_value=state):
            result = stream._effective_start_date(None)
        assert result == date(2024, 1, 16)


def _make_forecast_stream(*, bulk: bool = False) -> ForecastStream:
    """Return a ForecastStream instance backed by minimal mocked tap/config."""
    tap = MagicMock()
    tap.config = {
        "api_key": "test",
        "locations": ["London"],
        "start_date": "2024-01-01",
        "use_bulk_requests": bulk,
        "forecast_days": 5,
        "bulk_request_chunk_size": 50,
    }
    tap.state = {}
    tap.catalog = MagicMock()
    tap.catalog.get_stream.return_value = None
    return ForecastStream(tap=tap)


def _mock_response(
    status_code: int,
    json_body: dict | None = None,
    url: str = "https://api.weatherapi.com/v1/forecast.json?q=90210",
    body: str | None = None,
) -> MagicMock:
    """Build a minimal mock requests.Response."""
    resp = MagicMock()
    resp.url = url
    resp.status_code = status_code
    resp.json.return_value = json_body or {}
    resp.request = MagicMock()
    resp.request.url = url
    resp.request.body = body
    return resp


class TestValidateResponse:
    """Unit tests for WeatherAPIStream.validate_response."""

    def test_1006_logs_warning_and_does_not_raise(self) -> None:
        """A 400/1006 response logs a warning and returns without raising."""
        stream = _make_forecast_stream()
        resp = _mock_response(
            400,
            {"error": {"code": 1006, "message": "No matching location found."}},
            url="https://api.weatherapi.com/v1/forecast.json?key=x&q=99999",
        )

        with patch.object(stream.logger, "warning") as mock_warn:
            stream.validate_response(resp)  # must not raise

        mock_warn.assert_called_once()
        assert "99999" in mock_warn.call_args[0][1]
        assert "forecast" in mock_warn.call_args[0][2]

    def test_408_raises_retriable_error(self) -> None:
        """A 408 timeout is raised as RetriableAPIError so the SDK will retry."""
        stream = _make_forecast_stream()
        resp = _mock_response(408)

        with pytest.raises(RetriableAPIError):
            stream.validate_response(resp)

    def test_other_400_still_raises(self) -> None:
        """A 400 with a non-1006 error code propagates as a fatal error."""
        stream = _make_forecast_stream()
        resp = _mock_response(400, {"error": {"code": 1005, "message": "API key invalid."}})
        resp.ok = False
        resp.reason = "Bad Request"

        with pytest.raises(FatalAPIError):
            stream.validate_response(resp)

    def test_bulk_1006_logs_all_locations_from_body(self) -> None:
        """Bulk 400/1006 logs every location in the rejected chunk, not just 'bulk'."""
        import json as _json

        stream = _make_forecast_stream(bulk=True)
        bulk_body = _json.dumps({"locations": [{"q": "11111"}, {"q": "22222"}]})
        resp = _mock_response(
            400,
            {"error": {"code": 1006, "message": "No matching location found."}},
            url="https://api.weatherapi.com/v1/forecast.json?key=x&q=bulk",
            body=bulk_body,
        )

        with patch.object(stream.logger, "warning") as mock_warn:
            stream.validate_response(resp)  # must not raise

        mock_warn.assert_called_once()
        warning_location_arg = mock_warn.call_args[0][1]
        assert "11111" in warning_location_arg
        assert "22222" in warning_location_arg


class TestParseResponse:
    """Unit tests for WeatherAPIStream.parse_response."""

    def test_error_response_yields_nothing(self) -> None:
        """An error JSON response (e.g. after a skipped 1006) yields no records."""
        stream = _make_forecast_stream()
        resp = _mock_response(
            400, {"error": {"code": 1006, "message": "No matching location found."}}
        )

        records = list(stream.parse_response(resp))
        assert records == []

    def test_bulk_per_entry_error_is_skipped_and_valid_entries_yielded(self) -> None:
        """In a bulk 200 response, error entries are skipped and valid ones yield records."""
        stream = _make_forecast_stream(bulk=True)
        bulk_body = {
            "bulk": [
                {
                    "query": {
                        "q": "90210",
                        "custom_id": None,
                        "location": {"name": "Beverly Hills", "region": "California", "country": "USA", "lat": 34.1, "lon": -118.4, "tz_id": "America/Los_Angeles"},
                        "forecast": {
                            "forecastday": [
                                {"date": "2024-01-01", "date_epoch": 1704067200, "day": {}, "astro": {}, "hour": []}
                            ]
                        },
                    }
                },
                {
                    "query": {
                        "q": "99999",
                        "custom_id": None,
                        "error": {"code": 1006, "message": "No matching location found."},
                    }
                },
            ]
        }
        resp = _mock_response(200, bulk_body)

        with patch.object(stream.logger, "warning") as mock_warn:
            records = list(stream.parse_response(resp))

        assert len(records) == 1
        assert records[0]["location"] == "90210"
        mock_warn.assert_called_once()
        assert "99999" in mock_warn.call_args[0][2]
