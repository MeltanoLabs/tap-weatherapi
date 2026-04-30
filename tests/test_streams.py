"""Tests for stream-level behaviour (no real API calls required)."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from tap_weatherapi.streams import HistoricalStream


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
