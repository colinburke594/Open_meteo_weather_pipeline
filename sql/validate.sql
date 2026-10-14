-- Validation queries for the Open-Meteo pipeline.
-- Run with:  psql "$DATABASE_URL" -f sql/validate.sql
--
-- Rerun (idempotency) test:
--   1. Run this file and note the totals from query 1.
--   2. Re-run the same load, e.g.  python main.py --refresh-all --start 2026-10-01 --end 2026-10-07 --full-refresh
--   3. Run this file again. Row totals must be unchanged and queries 2-3 must return no rows.
--      (pipeline_runs WILL grow: every run is logged, by design.)

-- Show and group timestamps in UTC, like the pipeline stores them.
SET TIME ZONE 'UTC';

\echo '1. Rows per city: how many hours stored, over what range, and how complete'
SELECT c.city_id, c.city_name, c.country_code,
       COUNT(f.observed_at)                                   AS hours_stored,
       MIN(f.observed_at)                                     AS first_hour,
       MAX(f.observed_at)                                     AS last_hour,
       EXTRACT(EPOCH FROM MAX(f.observed_at) - MIN(f.observed_at)) / 3600 + 1
                                                              AS hours_in_range
FROM dim_city c
LEFT JOIN fact_weather_hourly f USING (city_id)
GROUP BY c.city_id, c.city_name, c.country_code
ORDER BY c.city_id;

\echo '2. Duplicate hours (expect 0 rows; the primary key should make this impossible)'
SELECT city_id, observed_at, COUNT(*)
FROM fact_weather_hourly
GROUP BY city_id, observed_at
HAVING COUNT(*) > 1;

\echo '3. Duplicate cities (expect 0 rows)'
SELECT geocoding_id, COUNT(*)
FROM dim_city
GROUP BY geocoding_id
HAVING COUNT(*) > 1;

\echo '4. Gaps: hours missing between each city''s first and last stored hour'
SELECT c.city_name, COUNT(*) AS missing_hours, MIN(h.hour) AS first_missing
FROM (SELECT city_id, MIN(observed_at) AS lo, MAX(observed_at) AS hi
      FROM fact_weather_hourly GROUP BY city_id) r
CROSS JOIN LATERAL generate_series(r.lo, r.hi, interval '1 hour') AS h(hour)
JOIN dim_city c USING (city_id)
WHERE NOT EXISTS (SELECT 1 FROM fact_weather_hourly f
                  WHERE f.city_id = r.city_id AND f.observed_at = h.hour)
GROUP BY c.city_name;

\echo '5. NULL measurements per city'
SELECT c.city_name,
       COUNT(*) FILTER (WHERE temperature_c IS NULL)         AS null_temp,
       COUNT(*) FILTER (WHERE precipitation_mm IS NULL)      AS null_precip,
       COUNT(*) FILTER (WHERE relative_humidity_pct IS NULL) AS null_humidity,
       COUNT(*) FILTER (WHERE wind_speed_kmh IS NULL)        AS null_wind_speed,
       COUNT(*) FILTER (WHERE wind_direction_deg IS NULL)    AS null_wind_dir
FROM fact_weather_hourly f JOIN dim_city c USING (city_id)
GROUP BY c.city_name;

\echo '6. Latest run per city'
SELECT DISTINCT ON (c.city_name)
       c.city_name, r.run_id, r.status, r.load_mode, r.window_start, r.window_end,
       r.rows_processed, r.finished_at - r.started_at AS duration, r.error_message
FROM pipeline_runs r JOIN dim_city c USING (city_id)
ORDER BY c.city_name, r.started_at DESC;

\echo '7. Runs that never finished (expect 0 rows unless a run is in progress)'
SELECT run_id, city_id, started_at FROM pipeline_runs WHERE status = 'running';

\echo '8. Sample analysis: daily summary for the last 7 stored days'
SELECT c.city_name, f.observed_at::date AS day,
       ROUND(MIN(temperature_c)::numeric, 1)  AS min_temp_c,
       ROUND(MAX(temperature_c)::numeric, 1)  AS max_temp_c,
       ROUND(SUM(precipitation_mm)::numeric, 1) AS precip_mm,
       ROUND(MAX(wind_speed_kmh)::numeric, 1) AS max_wind_kmh
FROM fact_weather_hourly f JOIN dim_city c USING (city_id)
WHERE f.observed_at >= (SELECT MAX(observed_at) FROM fact_weather_hourly) - interval '7 days'
GROUP BY c.city_name, day
ORDER BY c.city_name, day;
