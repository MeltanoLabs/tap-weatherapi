"""Tests standard tap features using the built-in SDK tests library."""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
import requests
from singer_sdk.testing import get_tap_test_class

from tap_weatherapi.streams import DateRangePaginator, DateWindow
from tap_weatherapi.tap import TapWeatherAPI

SAMPLE_CONFIG = {
    "locations": [
        "60605",
    ],
    "start_date": (datetime.now(timezone.utc).date() - timedelta(days=7)).isoformat(),
    "forecast_days": 5,
}


# Run standard built-in tap tests from the SDK:
TestTapWeatherAPI = get_tap_test_class(
    tap_class=TapWeatherAPI,
    config=SAMPLE_CONFIG,
)


# ---------------------------------------------------------------------------
# DateRangePaginator unit tests
# ---------------------------------------------------------------------------

_FAKE_RESPONSE = MagicMock(spec=requests.Response)  # parse_response is not called by the paginator


class TestDateRangePaginator:
    """Unit tests for DateRangePaginator."""

    def test_single_window(self) -> None:
        """A range that fits in one 30-day window produces exactly one page."""
        start = date(2024, 1, 1)
        end = date(2024, 1, 20)
        p = DateRangePaginator(start, end)

        assert not p.finished
        assert p.current_value == DateWindow(
            start=date(2024, 1, 1),
            end=date(2024, 1, 20),
        )

        p.advance(_FAKE_RESPONSE)
        assert p.finished

    def test_window_end_is_capped_at_end_date(self) -> None:
        """The last window's end is capped at end_date, not start + window_size - 1."""
        p = DateRangePaginator(date(2024, 1, 1), date(2024, 1, 10))
        assert p.current_value.end == date(2024, 1, 10)

    def test_multiple_windows(self) -> None:
        """A range spanning multiple windows carries correct start/end per window."""
        start = date(2024, 1, 1)
        end = date(2024, 3, 1)  # 61 days later → 3 windows
        p = DateRangePaginator(start, end)

        pages = [p.current_value]
        p.advance(_FAKE_RESPONSE)
        pages.append(p.current_value)
        p.advance(_FAKE_RESPONSE)
        pages.append(p.current_value)
        p.advance(_FAKE_RESPONSE)

        assert p.finished
        assert pages == [
            DateWindow(
                start=date(2024, 1, 1),
                end=date(2024, 1, 30),
            ),
            DateWindow(
                start=date(2024, 1, 31),
                end=date(2024, 2, 29),
            ),
            DateWindow(
                start=date(2024, 3, 1),
                end=date(2024, 3, 1),
            ),
        ]

    def test_exact_30_day_boundary(self) -> None:
        """A range of exactly 30 days produces two pages."""
        p = DateRangePaginator(
            date(2024, 1, 1),
            date(2024, 1, 31),
        )

        assert p.current_value == DateWindow(
            start=date(2024, 1, 1),
            end=date(2024, 1, 30),
        )
        p.advance(_FAKE_RESPONSE)
        assert p.current_value == DateWindow(
            start=date(2024, 1, 31),
            end=date(2024, 1, 31),
        )
        p.advance(_FAKE_RESPONSE)
        assert p.finished

    def test_start_after_end_is_immediately_finished(self) -> None:
        """When start_date > end_date no requests should be made."""
        p = DateRangePaginator(date(2025, 1, 1), date(2024, 1, 1))
        assert p.finished

    def test_start_equals_end(self) -> None:
        """A single-day range produces one page with start == end."""
        d = date(2024, 6, 15)
        p = DateRangePaginator(d, d)

        assert not p.finished
        assert p.current_value == DateWindow(start=d, end=d)
        p.advance(_FAKE_RESPONSE)
        assert p.finished

    def test_custom_window_size(self) -> None:
        """window_size is respected when set to a non-default value."""
        p = DateRangePaginator(date(2024, 1, 1), date(2024, 1, 14), window_size=7)

        assert p.current_value == DateWindow(
            start=date(2024, 1, 1),
            end=date(2024, 1, 7),
        )
        p.advance(_FAKE_RESPONSE)
        assert p.current_value == DateWindow(
            start=date(2024, 1, 8),
            end=date(2024, 1, 14),
        )
        p.advance(_FAKE_RESPONSE)
        assert p.finished

    @pytest.mark.parametrize(
        ("start", "end", "expected_pages"),
        [
            (
                date(2024, 1, 1),
                date(2024, 1, 1),
                1,
            ),  # 1 day
            (
                date(2024, 1, 1),
                date(2024, 1, 30),
                1,
            ),  # 30 days - one full window
            (
                date(2024, 1, 1),
                date(2024, 1, 31),
                2,
            ),  # 31 days - spills into second window
            (
                date(2024, 1, 1),
                date(2024, 3, 31),
                4,
            ),  # 90 days → 4 windows (Jan 1, Jan 31, Mar 1, Mar 31)
        ],
    )
    def test_page_count(self, start: date, end: date, expected_pages: int) -> None:
        """Test the number of pages produced by the paginator."""
        p = DateRangePaginator(start, end)
        count = 0
        while not p.finished:
            count += 1
            p.advance(_FAKE_RESPONSE)
        assert count == expected_pages
