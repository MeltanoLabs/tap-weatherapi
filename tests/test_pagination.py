"""Tests pagination helpers."""

import sys
from datetime import date
from typing import Any
from unittest.mock import MagicMock

import pytest
import requests
from singer_sdk.pagination import BasePageNumberPaginator

from tap_weatherapi.client import BulkChunkPaginationWrapper, _chunk_locations
from tap_weatherapi.streams import DateRangePaginator, DateWindow

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

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


class TestBulkChunkPaginationWrapper:  # noqa: D101
    def test_chunk_paginator(self) -> None:  # noqa: D102
        class MyPaginator(BasePageNumberPaginator):
            @override
            def get_next(self, response: requests.Response) -> int | None:
                return self._value + 1 if self._value < 2 else None  # noqa: PLR2004

        wrapped = MyPaginator(start_value=1)
        chunks = [
            [{"id": 1}, {"id": 2}],
            [{"id": 3}, {"id": 4}],
        ]

        paginator = BulkChunkPaginationWrapper(wrapped=wrapped, chunks=chunks)
        assert paginator.current_value.data == chunks[0]
        assert paginator.current_value.current_value == 1

        response = requests.Response()
        paginator.advance(response)
        assert paginator.current_value.data == chunks[0]
        assert paginator.current_value.current_value == 2  # noqa: PLR2004

        paginator.advance(response)
        assert paginator.current_value.data == chunks[1]
        assert paginator.current_value.current_value == 1

        paginator.advance(response)
        assert paginator.current_value.data == chunks[1]
        assert paginator.current_value.current_value == 2  # noqa: PLR2004

        paginator.advance(response)
        assert paginator.finished

    def test_with_date_range_paginator(self) -> None:
        """BulkChunkPaginationWrapper threads DateWindow values correctly across chunks.

        Specifically: the first page for every chunk must carry the *start* date
        (2026-01-01 in the scenario below), not start+1 or any other shifted value.
        This guards against the bug where missing rows appeared for the first day of
        the configured start_date in bulk mode.
        """
        start = date(2026, 1, 1)
        end = date(2026, 4, 27)
        inner = DateRangePaginator(start_date=start, end_date=end)
        chunks = [
            [{"location": "33807"}, {"location": "90210"}],
            [{"location": "Iztapalapa"}],
        ]
        paginator = BulkChunkPaginationWrapper(wrapped=inner, chunks=chunks)

        pages: list[tuple[list[dict[str, Any]], DateWindow]] = []
        while not paginator.finished:
            pages.append((paginator.current_value.data, paginator.current_value.current_value))
            paginator.advance(_FAKE_RESPONSE)

        expected_windows = [
            DateWindow(start=date(2026, 1, 1), end=date(2026, 1, 30)),
            DateWindow(start=date(2026, 1, 31), end=date(2026, 3, 1)),
            DateWindow(start=date(2026, 3, 2), end=date(2026, 3, 31)),
            DateWindow(start=date(2026, 4, 1), end=date(2026, 4, 27)),
        ]
        assert pages == [
            *[(chunks[0], w) for w in expected_windows],
            *[(chunks[1], w) for w in expected_windows],
        ]


_LOCS = [{"location": str(i)} for i in range(120)]


@pytest.mark.parametrize(
    ("chunk_size", "expected_chunks", "expected_last_len"),
    [
        (50, 3, 20),  # 120 / 50 → 2 full + 1 of 20
        (10, 12, 10),  # 120 / 10 → 12 full
        (7, 18, 1),  # 120 / 7 → 17 full (119) + 1 of 1
        (5, 24, 5),  # 120 / 5 → 24 full
    ],
)
def test_chunk_locations_sizes(
    chunk_size: int,
    expected_chunks: int,
    expected_last_len: int,
) -> None:
    chunks = _chunk_locations(_LOCS, chunk_size)
    assert len(chunks) == expected_chunks
    assert all(len(c) <= chunk_size for c in chunks)
    assert len(chunks[-1]) == expected_last_len


def test_chunk_locations_preserves_order() -> None:
    chunks = _chunk_locations(_LOCS, 50)
    flat = [loc for chunk in chunks for loc in chunk]
    assert flat == _LOCS


def test_chunk_locations_empty() -> None:
    assert _chunk_locations([], 50) == []


def test_chunk_locations_smaller_than_chunk() -> None:
    chunks = _chunk_locations([{"location": "A"}, {"location": "B"}], 50)
    assert chunks == [[{"location": "A"}, {"location": "B"}]]
