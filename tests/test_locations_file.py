"""Test locations input from a file."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from tap_weatherapi.client import _get_location_from_file

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    ("filename", "content", "locations"),
    [
        pytest.param(
            "locations.json",
            (
                '[{"location": "90210", "custom_id": "B.H."},'
                '{"location": "New York", "custom_id": "N.Y."}]'
            ),
            [
                {"location": "90210", "custom_id": "B.H."},
                {"location": "New York", "custom_id": "N.Y."},
            ],
            id="json-array",
        ),
        pytest.param(
            "locations.jsonl",
            (
                '{"location": "90210", "custom_id": "B.H."}\n'
                '{"location": "New York", "custom_id": "N.Y."}'
            ),
            [
                {"location": "90210", "custom_id": "B.H."},
                {"location": "New York", "custom_id": "N.Y."},
            ],
            id="json-array",
        ),
    ],
)
def test_get_location_from_file(
    tmp_path: Path,
    filename: str,
    content: str,
    locations: list[dict[str, Any]],
) -> None:
    """Validate reading locations from a .json or .jsonl file."""
    _get_location_from_file.cache_clear()
    path = tmp_path / filename
    path.write_text(content)
    assert _get_location_from_file(path) == locations
