# Open-Meteo weather pipeline

[![tests](https://github.com/colinburke594/Open_meteo_weather_pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/colinburke594/Open_meteo_weather_pipeline/actions/workflows/tests.yml)

An ETL pipeline that loads hourly historical weather from the
[Open-Meteo](https://open-meteo.com/) archive API into a PostgreSQL star schema.

- **Incremental.** Each run loads only the hours that are new, and re-reads recent hours that the provider may still revise.
- **Idempotent.** Re-running a load replaces rows instead of duplicating them.
- **Observable.** Every run is logged in `pipeline_runs` with its status, row count and data-quality counts.
- **Three ways to run:** interactive, a CSV batch, or a refresh of every stored city.

## Architecture

```mermaid
flowchart LR
    subgraph Input
        A1[Interactive prompt]
        A2[cities.csv]
        A3[--refresh-all<br/>stored cities]
    end

    G[(Open-Meteo<br/>Geocoding API)]
    W[(Open-Meteo<br/>Archive API)]

    subgraph Pipeline
        P[Plan window<br/><i>pipeline.py</i>]
        E[Extract<br/><i>extract.py</i>]
        T[Transform + validate<br/><i>transform.py</i>]
        L[Load: upsert<br/><i>load.py</i>]
    end

    subgraph PostgreSQL
        D[dim_city]
        F[fact_weather_hourly]
        R[pipeline_runs]
    end

    A1 & A2 --> G --> P
    A3 --> P
    D -. stored hours .-> P
    P --> E --> W
    W --> E --> T --> L
    L --> D & F
    P & L -. run status .-> R
```

For each city:

1. **Resolve.** The Geocoding API turns a name like "San Jose, California" into one place with a stable ID and coordinates. The city is upserted into `dim_city`.
2. **Plan.** The pipeline checks which hours are already stored and picks the date range still needed: a full load, an incremental load, or a skip.
3. **Extract.** One request to the Archive API, retried automatically on rate limits and server errors.
4. **Transform.** Problems with the whole response fail the run: wrong units, mismatched arrays, duplicate or out-of-range timestamps. A single bad value is stored as NULL and counted.
5. **Load.** The rows are bulk-loaded with `COPY` into a staging table, then upserted into `fact_weather_hourly` in one transaction.
6. **Log.** The run is recorded in `pipeline_runs` as `success`, `warning`, `skipped` or `failed`.

### Data model

| Table | Grain | Key |
|---|---|---|
| `dim_city` | One row per place | `city_id`; unique `geocoding_id` |
| `fact_weather_hourly` | One row per city per UTC hour | `(city_id, observed_at)` |
| `pipeline_runs` | One row per city per run | `run_id` |

The measurements stored are temperature (°C), precipitation (mm), relative humidity (%), wind speed (km/h) and wind direction (°).

## Installation

**Prerequisites:** Python 3.10+ and PostgreSQL 13+.

```bash
git clone https://github.com/colinburke594/Open_meteo_weather_pipeline.git
cd Open_meteo_weather_pipeline

python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

createdb weather                   # or CREATE DATABASE weather; in psql
```

## Configuration

Copy the example environment file and set your connection string:

```bash
cp .env.example .env
```

| Variable | Required | Purpose |
|---|---|---|
| `DATABASE_URL` | Yes | e.g. `postgresql://user:password@localhost:5432/weather` |
| `TEST_DATABASE_URL` | No | A disposable database for the integration tests (its name must contain `test`) |

`.env` is git-ignored. The tables are created automatically on the first run.

## Usage

```bash
python main.py                                  # interactive: search for a city, pick dates
python main.py --cities cities.example.csv      # batch: every city in a CSV, no prompts
python main.py --refresh-all                    # incremental update of every stored city
python main.py --refresh-all --start 2026-01-01 --end 2026-03-31 --full-refresh
```

| Option | Default | Meaning |
|---|---|---|
| `--start`, `--end` | Resume from stored data / yesterday | Inclusive UTC dates |
| `--full-refresh` | Off | Reload the whole range, even if already stored |
| `--initial-days` | 30 | History loaded for a new city when no `--start` is given |
| `--lookback-days` | 5 | Rows loaded within this many days of their hour are provisional and re-read later |
| `--log-level` | INFO | DEBUG, INFO, WARNING or ERROR |

The CSV columns are `city,state,country,geocoding_id`. `geocoding_id` is optional, but it pins the exact place when a name matches several.

Example output:

```
INFO    src.pipeline: San Jose: full load 2026-10-01 -> 2026-10-07 (run_id=1).
INFO    src.pipeline: San Jose: committed 168 hourly rows (status=success, run_id=1).
INFO    src.pipeline: New York: full load 2026-10-01 -> 2026-10-07 (run_id=2).
INFO    src.pipeline: New York: committed 168 hourly rows (status=success, run_id=2).

city                     status        rows  run_id
San Jose                 success        168  1
New York                 success        168  2
Done: 2 success. Details in public.pipeline_runs.
```

### Incremental loading

Without `--start`, a city resumes right after its latest stored hour. The
archive publishes with a delay and may revise recent values. Rows loaded within
`--lookback-days` of their hour therefore count as provisional, and the next run
downloads them again. An explicit range that is already stored is skipped. A
range that starts before any stored data is loaded in full.

### Data quality

A missing or invalid measurement is stored as NULL and counted. It does not fail
the run. Hours with no usable measurement are not loaded (usually they are not
published yet) and are retried on a later run. Hours missing from the response
are reported as gaps. The counts are saved in `pipeline_runs`, and the run's
status is `warning` instead of `success`.

## Sample queries

[`sql/validate.sql`](sql/validate.sql) checks the loaded data: rows per city,
duplicates, gaps, NULLs and the latest run per city. It also includes a daily
summary.

```bash
psql "$DATABASE_URL" -f sql/validate.sql
```

```sql
-- Latest run per city
SELECT DISTINCT ON (c.city_name) c.city_name, r.status, r.started_at,
       r.finished_at - r.started_at AS duration, r.rows_processed, r.error_message
FROM pipeline_runs r JOIN dim_city c USING (city_id)
ORDER BY c.city_name, r.started_at DESC;

-- Daily temperature range and rainfall
SELECT c.city_name, f.observed_at::date AS day,
       MIN(temperature_c) AS min_temp_c, MAX(temperature_c) AS max_temp_c,
       SUM(precipitation_mm) AS precip_mm
FROM fact_weather_hourly f JOIN dim_city c USING (city_id)
GROUP BY c.city_name, day
ORDER BY c.city_name, day;

-- Runs with data-quality issues
SELECT run_id, city_id, missing_hours, unavailable_hours, null_counts, invalid_counts
FROM pipeline_runs WHERE status = 'warning' ORDER BY run_id DESC;
```

To check idempotency, run a load, note the row counts, re-run the same load
with `--full-refresh`, and compare. The counts stay the same; only
`pipeline_runs` grows.

## Tests

```bash
pip install -r requirements-dev.txt
pytest                                                          # unit tests
TEST_DATABASE_URL=postgresql://.../weather_test pytest          # + PostgreSQL integration tests
```

The integration tests drop and recreate the pipeline tables. They refuse to run
unless the database name contains `test`. GitHub Actions runs both suites on every push
(see [`.github/workflows/tests.yml`](.github/workflows/tests.yml)).

## Project layout

| Path | Responsibility |
|---|---|
| `main.py` | Command-line entry point |
| `src/geocoding.py` | Geocoding API search and lookup |
| `src/cli.py` | Interactive prompts (the only module that calls `input()`) |
| `src/extract.py` | HTTP session with retries; Archive API request |
| `src/transform.py` | Validation and data-quality report |
| `src/load.py` | Schema, COPY-based bulk upserts, run log |
| `src/pipeline.py` | Incremental window planning and per-city orchestration |
| `sql/schema.sql` | Idempotent DDL, including the v1 → v2 nullable-column migration |
| `sql/validate.sql` | Validation and sample analysis queries |
| `tests/` | Unit tests and PostgreSQL integration tests |

## Notes

Archive values are modelled reanalysis data, not necessarily station
measurements. All times are UTC. Weather data is provided by
[Open-Meteo](https://open-meteo.com/) under CC BY 4.0.

## License

[MIT](LICENSE)
