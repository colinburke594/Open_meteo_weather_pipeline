# Open-Meteo weather pipeline

Hourly historical weather from the Open-Meteo archive API into a PostgreSQL star
schema (`dim_city`, `fact_weather_hourly`), with a run log (`pipeline_runs`).

## Layout

| Path | Responsibility |
|---|---|
| `main.py` | Command-line entry point: interactive, CSV batch, or refresh-all |
| `src/geocoding.py` | Geocoding API search/lookup (no user input) |
| `src/cli.py` | Interactive prompts (the only module that calls `input()`) |
| `src/extract.py` | HTTP session with retries, archive API request |
| `src/transform.py` | Validation and data-quality report |
| `src/load.py` | Schema, COPY-based bulk upserts, run log |
| `src/pipeline.py` | Incremental window planning and per-city orchestration |
| `sql/schema.sql` | Idempotent DDL, including the v1 → v2 nullable-column migration |
| `archive/pipeline_v1.py` | The original single-file version, for reference |

## Run

```bash
pip install -r requirements.txt
cp .env.example .env              # set DATABASE_URL

python main.py                                 # interactive, one city
python main.py --cities cities.example.csv     # batch, no prompts
python main.py --refresh-all                   # incremental update of every stored city
python main.py --refresh-all --start 2026-01-01 --end 2026-03-31 --full-refresh
```

**Incremental loading.** Without `--start`, a city resumes right after its latest
stored hour. A new city gets the last `--initial-days` days (default 30). Rows
loaded within `--lookback-days` (default 5) of their hour count as provisional,
because the archive may still revise them, and later runs re-read them. An
explicit range that is already stored is skipped. Use `--full-refresh` to
force a reload.

**Missing data.** A missing or invalid measurement is stored as NULL and
counted. It does not fail the run. Hours with no usable measurement are not
loaded (usually they are not yet published). Timestamps missing from the API
response are reported as gaps. Structural problems (units, array lengths,
duplicate or out-of-range timestamps) still fail the run.

## Monitoring

```sql
-- Latest run per city
SELECT DISTINCT ON (c.city_name) c.city_name, r.status, r.started_at,
       r.finished_at - r.started_at AS duration, r.rows_processed, r.error_message
FROM pipeline_runs r JOIN dim_city c USING (city_id)
ORDER BY c.city_name, r.started_at DESC;

-- Runs with data-quality issues
SELECT run_id, city_id, missing_hours, unavailable_hours, null_counts, invalid_counts
FROM pipeline_runs WHERE status = 'warning' ORDER BY run_id DESC;
```

## Tests

```bash
pytest                                            # unit tests
TEST_DATABASE_URL=postgresql://.../weather_test pytest   # + PostgreSQL integration tests
```

The integration tests drop and recreate the pipeline tables. They refuse to run
unless the database name contains `test`.
# Open_meteo_weather_pipeline
