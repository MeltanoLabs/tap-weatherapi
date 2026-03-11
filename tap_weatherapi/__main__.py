"""WeatherAPI entry point."""

from __future__ import annotations

from tap_weatherapi.tap import TapWeatherAPI

TapWeatherAPI.cli()
