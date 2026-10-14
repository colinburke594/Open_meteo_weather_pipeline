"""Integration tests against a real, disposable PostgreSQL database.

Set TEST_DATABASE_URL to a database whose name contains "test"; its pipeline
tables are dropped and recreated. Skipped when the variable is not set.
"""
import os
from datetime import date

import psycopg
import pytest

from src import load, pipeline
from src.transform import transform_weather
from tests.test_transform import make_raw

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not TEST_DATABASE_URL, reason="TEST_DATABASE_URL not set")

CITY = {"id": 5392171, "name": "San Jose", "admin1": "California", "country": "United States",
        "country_code": "US", "latitude": 37.33939, "longitude": -121.89496,
        "timezone": "America/Los_Angeles"}
DAY = date(2026, 1, 1)

V1_FACT_TABLE = """
CREATE TABLE public.fact_weather_hourly (
    city_id BIGINT NOT NULL REFERENCES public.dim_city(city_id),
    observed_at TIMESTAMPTZ NOT NULL,
    temperature_c DOUBLE PRECISION NOT NULL,
    precipitation_mm DOUBLE PRECISION NOT NULL CHECK (precipitation_mm >= 0),
    relative_humidity_pct DOUBLE PRECISION NOT NULL CHECK (relative_humidity_pct BETWEEN 0 AND 100),
    wind_speed_kmh DOUBLE PRECISION NOT NULL CHECK (wind_speed_kmh >= 0),
    wind_direction_deg DOUBLE PRECISION NOT NULL CHECK (wind_direction_deg BETWEEN 0 AND 360),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city_id, observed_at)
)
"""


@pytest.fixture
def conn():
    with load.connect(TEST_DATABASE_URL) as conn:
        if "test" not in conn.info.dbname:
            pytest.fail("Refusing to drop tables: TEST_DATABASE_URL database name must contain 'test'.")
        conn.execute("DROP TABLE IF EXISTS public.pipeline_runs, public.fact_weather_hourly, "
                     "public.dim_city")
        load.ensure_schema(conn)
        yield conn


def count(conn, table):
    return conn.execute(f"SELECT COUNT(*) FROM public.{table}").fetchone()[0]


def test_schema_is_idempotent(conn):
    load.ensure_schema(conn)
    load.ensure_schema(conn)


def test_schema_migrates_v1_not_null_columns(conn):
    conn.execute("DROP TABLE public.pipeline_runs, public.fact_weather_hourly")
    conn.execute(V1_FACT_TABLE)
    load.ensure_schema(conn)
    raw = make_raw()
    raw["hourly"]["wind_speed_10m"][3] = None
    city_id = load.upsert_city(conn, CITY)
    load.load_observations(conn, city_id, transform_weather(raw, DAY, DAY).frame)
    nulls = conn.execute("SELECT COUNT(*) FROM public.fact_weather_hourly "
                         "WHERE wind_speed_kmh IS NULL").fetchone()[0]
    assert nulls == 1


def test_rerun_upserts_without_duplicates(conn):
    city_id = load.upsert_city(conn, CITY)
    assert load.upsert_city(conn, CITY) == city_id
    raw = make_raw()
    assert load.load_observations(conn, city_id, transform_weather(raw, DAY, DAY).frame) == 24
    raw["hourly"]["temperature_2m"] = [12.5] * 24
    assert load.load_observations(conn, city_id, transform_weather(raw, DAY, DAY).frame) == 24
    assert count(conn, "fact_weather_hourly") == 24
    assert conn.execute("SELECT MIN(temperature_c) FROM public.fact_weather_hourly").fetchone()[0] == 12.5


def test_failed_load_rolls_back(conn):
    city_id = load.upsert_city(conn, CITY)
    frame = transform_weather(make_raw(), DAY, DAY).frame
    frame.loc[5, "relative_humidity_pct"] = 150.0  # bypasses transform; DB CHECK rejects it
    with pytest.raises(psycopg.errors.CheckViolation):
        load.load_observations(conn, city_id, frame)
    assert count(conn, "fact_weather_hourly") == 0


def test_run_city_logs_runs_and_skips_loaded_ranges(conn, monkeypatch):
    raw = make_raw()
    raw["hourly"]["precipitation"][7] = None
    calls = []

    def fake_fetch(session, latitude, longitude, start, end):
        calls.append((start, end))
        return raw

    monkeypatch.setattr(pipeline, "fetch_weather", fake_fetch)
    first = pipeline.run_city(conn, None, CITY, DAY, DAY)
    assert (first.status, first.rows) == ("warning", 24)
    second = pipeline.run_city(conn, None, CITY, DAY, DAY)
    assert second.status == "skipped"
    assert len(calls) == 1

    run = conn.execute("""
        SELECT status, load_mode, rows_processed, expected_hours, null_counts, finished_at IS NOT NULL
        FROM public.pipeline_runs WHERE run_id = %s
    """, (first.run_id,)).fetchone()
    assert run[:4] == ("warning", "full", 24, 24)
    assert run[4]["precipitation_mm"] == 1
    assert run[5] is True


def test_run_city_records_failures(conn, monkeypatch):
    def broken_fetch(*args):
        raw = make_raw()
        raw["hourly_units"]["temperature_2m"] = "°F"
        return raw

    monkeypatch.setattr(pipeline, "fetch_weather", broken_fetch)
    result = pipeline.run_city(conn, None, CITY, DAY, DAY)
    assert result.status == "failed"
    status, error = conn.execute("SELECT status, error_message FROM public.pipeline_runs "
                                 "WHERE run_id = %s", (result.run_id,)).fetchone()
    assert status == "failed" and "units" in error
    assert count(conn, "fact_weather_hourly") == 0


def test_recently_loaded_hours_are_provisional(conn):
    city_id = load.upsert_city(conn, CITY)
    load.load_observations(conn, city_id, transform_weather(make_raw(), DAY, DAY).frame)
    # Loaded long after the hour it describes -> final.
    assert load.loaded_state(conn, city_id, 5)[2] is None
    # Pretend hours from 12:00 on were loaded the same day -> still provisional.
    conn.execute("UPDATE public.fact_weather_hourly SET loaded_at = observed_at + interval '1 hour' "
                 "WHERE observed_at >= '2026-01-01 12:00+00'")
    loaded_min, loaded_max, provisional_from = load.loaded_state(conn, city_id, 5)
    assert provisional_from.hour == 12
    assert loaded_max.hour == 23
