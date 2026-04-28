"""Tests standard tap features using the built-in SDK tests library."""

from datetime import datetime, timedelta, timezone

from singer_sdk.testing import get_tap_test_class

from tap_weatherapi.tap import TapWeatherAPI

SAMPLE_CONFIG = {
    "locations": [
        "60605",
    ],
    "start_date": (datetime.now(timezone.utc).date() - timedelta(days=7)).isoformat(),
    "forecast_days": 5,
    "use_bulk_requests": True,
}


# Run standard built-in tap tests from the SDK:
TestTapWeatherAPI = get_tap_test_class(
    tap_class=TapWeatherAPI,
    config=SAMPLE_CONFIG,
)
