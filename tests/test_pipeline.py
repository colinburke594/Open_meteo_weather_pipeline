from datetime import date, datetime, timedelta, timezone

import pytest

from src.geocoding import filter_locations
from src.pipeline import plan_window, validate_dates, yesterday_utc

TODAY_END = date(2026, 10, 13)


def utc(year, month, day, hour=0):
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


def test_new_city_without_dates_loads_initial_history():
    window = plan_window(None, None, None, None, None, initial_days=30, today_end=TODAY_END)
    assert (window.start, window.end, window.mode) == (date(2026, 9, 14), TODAY_END, "full")


def test_new_city_with_dates_loads_requested_range():
    window = plan_window(None, None, None, date(2026, 1, 1), date(2026, 3, 31))
    assert (window.start, window.end, window.mode) == (date(2026, 1, 1), date(2026, 3, 31), "full")


def test_incremental_resumes_after_watermark():
    window = plan_window(utc(2026, 1, 1), utc(2026, 9, 30, 23), None, None, None,
                         today_end=TODAY_END)
    assert (window.start, window.end, window.mode) == (date(2026, 10, 1), TODAY_END, "incremental")


def test_stale_city_resumes_from_watermark_not_default_window():
    window = plan_window(utc(2026, 1, 1), utc(2026, 6, 30, 23), None, None, None,
                         initial_days=30, today_end=TODAY_END)
    assert (window.start, window.mode) == (date(2026, 7, 1), "incremental")


def test_incremental_resumes_within_partially_loaded_day():
    window = plan_window(utc(2026, 1, 1), utc(2026, 9, 30, 14), None, None, None,
                         today_end=TODAY_END)
    assert window.start == date(2026, 9, 30)


def test_incremental_rereads_provisional_hours():
    window = plan_window(utc(2026, 1, 1), utc(2026, 10, 10, 23), utc(2026, 10, 6, 0),
                         None, None, today_end=TODAY_END)
    assert (window.start, window.end, window.mode) == (date(2026, 10, 6), TODAY_END, "incremental")


def test_up_to_date_final_data_is_skipped():
    assert plan_window(utc(2026, 1, 1), utc(2026, 10, 13, 23), None, None, None,
                       today_end=TODAY_END) is None


def test_requested_range_already_stored_is_skipped():
    window = plan_window(utc(2026, 1, 1), utc(2026, 10, 10, 23), utc(2026, 10, 6),
                         date(2026, 2, 1), date(2026, 3, 1))
    assert window is None


def test_partially_stored_request_only_extracts_new_part():
    window = plan_window(utc(2026, 1, 1), utc(2026, 9, 1, 23), None,
                         date(2026, 1, 1), date(2026, 10, 1))
    assert (window.start, window.end, window.mode) == (date(2026, 9, 2), date(2026, 10, 1), "incremental")


def test_request_before_stored_history_loads_full_range():
    window = plan_window(utc(2026, 6, 1), utc(2026, 9, 1, 23), None,
                         date(2026, 1, 1), date(2026, 10, 1))
    assert (window.start, window.mode) == (date(2026, 1, 1), "full")


def test_full_refresh_ignores_stored_data():
    window = plan_window(utc(2026, 1, 1), utc(2026, 10, 10, 23), None,
                         date(2026, 2, 1), date(2026, 3, 1), full_refresh=True)
    assert (window.start, window.end, window.mode) == (date(2026, 2, 1), date(2026, 3, 1), "full")


@pytest.mark.parametrize("start, end", [
    (date(1939, 12, 31), None),
    (None, yesterday_utc() + timedelta(days=1)),
    (yesterday_utc() + timedelta(days=1), None),
    (date(2026, 2, 1), date(2026, 1, 1)),
])
def test_invalid_dates_rejected(start, end):
    with pytest.raises(ValueError):
        validate_dates(start, end)


def test_filter_locations_keeps_populated_places_and_dedupes():
    results = [
        {"id": 1, "name": "San Jose", "latitude": 37.3, "longitude": -121.9,
         "feature_code": "PPLA2", "country": "United States"},
        {"id": 1, "name": "San Jose", "latitude": 37.3, "longitude": -121.9,
         "feature_code": "PPLA2", "country": "United States"},
        {"id": 2, "name": "San Jose Peak", "latitude": 1, "longitude": 1,
         "feature_code": "MT", "country": "United States"},
        {"id": 3, "name": "San José", "latitude": 9.9, "longitude": -84.1,
         "feature_code": "PPLC", "country": "Costa Rica"},
    ]
    assert [r["id"] for r in filter_locations(results)] == [1, 3]
    assert [r["id"] for r in filter_locations(results, "costa rica")] == [3]


def test_filter_locations_rejects_invalid_coordinates():
    bad = [{"id": 9, "name": "Nowhere", "latitude": 95, "longitude": 0, "feature_code": "PPL"}]
    with pytest.raises(ValueError, match="coordinates"):
        filter_locations(bad)
