"""Load: PostgreSQL schema, dimension/fact upserts, and the run log.

Connections run in autocommit mode; every multi-statement write is wrapped in
an explicit ``conn.transaction()`` so it commits or rolls back as one unit.
Run-log writes are separate transactions on purpose: a failed data load is
rolled back, but the record that it failed is kept.
SQL values are always bound parameters, never formatted into SQL strings.
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd
import psycopg
from psycopg.types.json import Jsonb

from src.transform import MEASURES, QualityReport

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


def connect(database_url: str) -> psycopg.Connection:
    conn = psycopg.connect(database_url, connect_timeout=10, autocommit=True)
    conn.execute("SET TIME ZONE 'UTC'")
    return conn


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.transaction():
        conn.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def upsert_city(conn: psycopg.Connection, location: dict) -> int:
    """The unique provider ID prevents namesakes from being merged. ON CONFLICT
    returns the existing surrogate key safely even during concurrent loads."""
    row = conn.execute("""
        INSERT INTO public.dim_city
            (geocoding_id, city_name, state, country, country_code, latitude, longitude, timezone)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (geocoding_id) DO UPDATE SET
            city_name = EXCLUDED.city_name, state = EXCLUDED.state,
            country = EXCLUDED.country, country_code = EXCLUDED.country_code,
            latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
            timezone = EXCLUDED.timezone, updated_at = CURRENT_TIMESTAMP
        RETURNING city_id
    """, (int(location["id"]), location["name"], location.get("admin1"), location.get("country"),
          location.get("country_code"), float(location["latitude"]),
          float(location["longitude"]), location.get("timezone"))).fetchone()
    return row[0]


def list_cities(conn: psycopg.Connection) -> list[dict]:
    """Every stored city, shaped like a geocoding result so it can be re-run directly."""
    rows = conn.execute("""
        SELECT geocoding_id, city_name, state, country, country_code, latitude, longitude, timezone
        FROM public.dim_city ORDER BY city_id
    """).fetchall()
    keys = ("id", "name", "admin1", "country", "country_code", "latitude", "longitude", "timezone")
    return [dict(zip(keys, row)) for row in rows]


def loaded_state(conn: psycopg.Connection, city_id: int, provisional_days: int
                 ) -> tuple[datetime | None, datetime | None, datetime | None]:
    """Earliest stored hour, latest stored hour (the watermark), and earliest
    provisional hour for a city.

    A row is provisional if it was loaded less than ``provisional_days`` after
    the hour it describes: the archive may still have revised it since.
    """
    return conn.execute("""
        SELECT MIN(observed_at), MAX(observed_at),
               MIN(observed_at) FILTER (
                   WHERE loaded_at < observed_at + make_interval(days => %s))
        FROM public.fact_weather_hourly WHERE city_id = %s
    """, (provisional_days, city_id)).fetchone()


def load_observations(conn: psycopg.Connection, city_id: int, frame: pd.DataFrame) -> int:
    """Bulk-load via COPY into a temp staging table, then one set-based upsert.

    COPY streams rows in a single round trip instead of one INSERT per row,
    which matters once a backfill reaches tens of thousands of hours.
    """
    columns = ["observed_at", *MEASURES]
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE stage_weather_hourly (
                    observed_at TIMESTAMPTZ NOT NULL,
                    temperature_c DOUBLE PRECISION,
                    precipitation_mm DOUBLE PRECISION,
                    relative_humidity_pct DOUBLE PRECISION,
                    wind_speed_kmh DOUBLE PRECISION,
                    wind_direction_deg DOUBLE PRECISION
                ) ON COMMIT DROP
            """)
            with cur.copy(f"COPY stage_weather_hourly ({', '.join(columns)}) FROM STDIN") as copy:
                for row in frame[columns].itertuples(index=False):
                    copy.write_row([row[0].to_pydatetime(),
                                    *(None if pd.isna(v) else float(v) for v in row[1:])])
            cur.execute("""
                INSERT INTO public.fact_weather_hourly
                    (city_id, observed_at, temperature_c, precipitation_mm,
                     relative_humidity_pct, wind_speed_kmh, wind_direction_deg)
                SELECT %s, observed_at, temperature_c, precipitation_mm,
                       relative_humidity_pct, wind_speed_kmh, wind_direction_deg
                FROM stage_weather_hourly
                ON CONFLICT (city_id, observed_at) DO UPDATE SET
                    temperature_c = EXCLUDED.temperature_c,
                    precipitation_mm = EXCLUDED.precipitation_mm,
                    relative_humidity_pct = EXCLUDED.relative_humidity_pct,
                    wind_speed_kmh = EXCLUDED.wind_speed_kmh,
                    wind_direction_deg = EXCLUDED.wind_direction_deg,
                    loaded_at = CURRENT_TIMESTAMP
            """, (city_id,))
            return cur.rowcount


def start_run(conn: psycopg.Connection, city_id: int | None, load_mode: str,
              window_start: date | None, window_end: date | None) -> int:
    return conn.execute("""
        INSERT INTO public.pipeline_runs (city_id, status, load_mode, window_start, window_end)
        VALUES (%s, 'running', %s, %s, %s) RETURNING run_id
    """, (city_id, load_mode, window_start, window_end)).fetchone()[0]


def finish_run(conn: psycopg.Connection, run_id: int, status: str, rows_processed: int = 0,
               quality: QualityReport | None = None, error_message: str | None = None) -> None:
    conn.execute("""
        UPDATE public.pipeline_runs SET
            finished_at = CURRENT_TIMESTAMP, status = %s, rows_processed = %s,
            expected_hours = %s, missing_hours = %s, unavailable_hours = %s,
            null_counts = %s, invalid_counts = %s, error_message = %s
        WHERE run_id = %s
    """, (status, rows_processed,
          quality.expected_hours if quality else None,
          quality.missing_hours if quality else None,
          quality.unavailable_hours if quality else None,
          Jsonb(quality.null_counts) if quality else None,
          Jsonb(quality.invalid_counts) if quality else None,
          error_message, run_id))
