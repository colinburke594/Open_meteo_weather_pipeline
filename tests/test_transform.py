from datetime import date

import pandas as pd
import pytest

from src.extract import VARIABLES
from src.transform import EXPECTED_UNITS, transform_weather

START = END = date(2026, 1, 1)


def make_raw(hours=24, start="2026-01-01T00:00"):
    times = pd.date_range(start, periods=hours, freq="h").strftime("%Y-%m-%dT%H:%M").tolist()
    hourly = {"time": times,
              "temperature_2m": [10.0] * hours,
              "precipitation": [0.0] * hours,
              "relative_humidity_2m": [50.0] * hours,
              "wind_speed_10m": [5.0] * hours,
              "wind_direction_10m": [180.0] * hours}
    return {"utc_offset_seconds": 0, "hourly": hourly, "hourly_units": dict(EXPECTED_UNITS)}


def test_clean_day_loads_every_hour():
    result = transform_weather(make_raw(), START, END)
    assert len(result.frame) == 24
    assert not result.quality.has_issues
    assert result.quality.expected_hours == 24
    assert str(result.frame["observed_at"].dt.tz) == "UTC"


def test_one_missing_measurement_keeps_the_row():
    raw = make_raw()
    raw["hourly"]["wind_speed_10m"][13] = None
    result = transform_weather(raw, START, END)
    assert len(result.frame) == 24
    assert result.quality.null_counts["wind_speed_kmh"] == 1
    assert result.quality.null_counts["temperature_c"] == 0
    assert result.frame.loc[13, "temperature_c"] == 10.0
    assert pd.isna(result.frame.loc[13, "wind_speed_kmh"])
    assert result.quality.has_issues


def test_hours_with_no_measurements_are_not_loaded():
    raw = make_raw()
    for name in VARIABLES:
        for hour in (22, 23):
            raw["hourly"][name][hour] = None
    result = transform_weather(raw, START, END)
    assert len(result.frame) == 22
    assert result.quality.unavailable_hours == 2
    assert result.quality.null_counts == {c: 0 for c in VARIABLES.values()}


def test_missing_timestamp_is_reported_as_gap():
    raw = make_raw()
    for values in raw["hourly"].values():
        del values[5]
    result = transform_weather(raw, START, END)
    assert len(result.frame) == 23
    assert result.quality.missing_hours == 1
    assert result.quality.first_missing == [pd.Timestamp("2026-01-01T05:00", tz="UTC")]


@pytest.mark.parametrize("variable, value", [
    ("relative_humidity_2m", 101.0),
    ("wind_direction_10m", -1.0),
    ("precipitation", -0.1),
    ("wind_speed_10m", float("inf")),
    ("temperature_2m", "n/a"),
])
def test_invalid_value_is_nulled_and_counted(variable, value):
    raw = make_raw()
    raw["hourly"][variable][0] = value
    result = transform_weather(raw, START, END)
    column = VARIABLES[variable]
    assert len(result.frame) == 24
    assert pd.isna(result.frame.loc[0, column])
    assert result.quality.invalid_counts[column] == 1
    assert result.quality.null_counts[column] == 1


def test_range_boundaries_are_valid():
    raw = make_raw()
    raw["hourly"]["relative_humidity_2m"][0] = 100.0
    raw["hourly"]["wind_direction_10m"][0] = 360.0
    raw["hourly"]["precipitation"][0] = 0.0
    assert not transform_weather(raw, START, END).quality.has_issues


def test_wrong_units_fail_the_run():
    raw = make_raw()
    raw["hourly_units"]["temperature_2m"] = "°F"
    with pytest.raises(ValueError, match="units"):
        transform_weather(raw, START, END)


def test_non_utc_response_fails_the_run():
    raw = make_raw()
    raw["utc_offset_seconds"] = 3600
    with pytest.raises(ValueError, match="UTC"):
        transform_weather(raw, START, END)


def test_mismatched_array_lengths_fail_the_run():
    raw = make_raw()
    raw["hourly"]["precipitation"].pop()
    with pytest.raises(ValueError, match="lengths"):
        transform_weather(raw, START, END)


def test_duplicate_timestamps_fail_the_run():
    raw = make_raw()
    raw["hourly"]["time"][1] = raw["hourly"]["time"][0]
    with pytest.raises(ValueError, match="duplicate"):
        transform_weather(raw, START, END)


def test_timestamps_outside_window_fail_the_run():
    with pytest.raises(ValueError, match="outside"):
        transform_weather(make_raw(hours=25), START, END)


def test_missing_hourly_block_fails_the_run():
    raw = make_raw()
    del raw["hourly"]["wind_direction_10m"]
    with pytest.raises(ValueError, match="hourly arrays"):
        transform_weather(raw, START, END)
