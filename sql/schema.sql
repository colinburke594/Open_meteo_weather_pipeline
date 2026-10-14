-- Idempotent schema for the Open-Meteo pipeline. Safe to run on every start.
-- CREATE TABLE IF NOT EXISTS never alters an existing table, so changes to
-- existing tables are written as explicit, re-runnable ALTER statements below.
-- When the schema grows further, move these into a migration tool (Alembic,
-- sqitch, Flyway) instead of extending this file.

CREATE TABLE IF NOT EXISTS public.dim_city (
    city_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    geocoding_id BIGINT NOT NULL UNIQUE,
    city_name TEXT NOT NULL,
    state TEXT,
    country TEXT,
    country_code TEXT,
    latitude DOUBLE PRECISION NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    timezone TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Grain: one row per city per UTC hour. Measurements are nullable: a missing
-- wind reading should not discard an otherwise valid temperature reading.
-- CHECK constraints still apply to every value that is present.
CREATE TABLE IF NOT EXISTS public.fact_weather_hourly (
    city_id BIGINT NOT NULL REFERENCES public.dim_city(city_id),
    observed_at TIMESTAMPTZ NOT NULL,
    temperature_c DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION CHECK (precipitation_mm >= 0),
    relative_humidity_pct DOUBLE PRECISION CHECK (relative_humidity_pct BETWEEN 0 AND 100),
    wind_speed_kmh DOUBLE PRECISION CHECK (wind_speed_kmh >= 0),
    wind_direction_deg DOUBLE PRECISION CHECK (wind_direction_deg BETWEEN 0 AND 360),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city_id, observed_at)
);

-- Migration for tables created by pipeline v1, where measurements were NOT NULL.
-- DROP NOT NULL is a no-op on a column that is already nullable.
ALTER TABLE public.fact_weather_hourly
    ALTER COLUMN temperature_c DROP NOT NULL,
    ALTER COLUMN precipitation_mm DROP NOT NULL,
    ALTER COLUMN relative_humidity_pct DROP NOT NULL,
    ALTER COLUMN wind_speed_kmh DROP NOT NULL,
    ALTER COLUMN wind_direction_deg DROP NOT NULL;

-- Operational metadata, not part of the star schema: one row per city per run.
-- Answers "when did this last succeed?", "how many rows?", "what failed?".
CREATE TABLE IF NOT EXISTS public.pipeline_runs (
    run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    city_id BIGINT REFERENCES public.dim_city(city_id),
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'warning', 'skipped', 'failed')),
    load_mode TEXT NOT NULL CHECK (load_mode IN ('incremental', 'full')),
    window_start DATE,
    window_end DATE,
    rows_processed INTEGER NOT NULL DEFAULT 0,
    expected_hours INTEGER,
    missing_hours INTEGER,       -- timestamps absent from the API response (gaps)
    unavailable_hours INTEGER,   -- timestamps present but with every measurement null
    null_counts JSONB,           -- per-measurement nulls among loaded rows
    invalid_counts JSONB,        -- per-measurement values rejected by validation
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS pipeline_runs_city_started_idx
    ON public.pipeline_runs (city_id, started_at DESC);
