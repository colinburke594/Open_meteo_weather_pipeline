"""Orchestration: decide what to extract, run E -> T -> L, and log the run."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import psycopg
import requests

from src import load
from src.extract import fetch_weather
from src.transform import QualityReport, transform_weather

log = logging.getLogger(__name__)

EARLIEST_ARCHIVE_DATE = date(1940, 1, 1)
# The archive publishes with a delay of several days and may revise recent
# values. Rows loaded within this many days of their hour are provisional, and
# incremental runs re-read them until they have been loaded after that window.
DEFAULT_LOOKBACK_DAYS = 5
# History loaded for a city that has no stored data and no requested start date.
DEFAULT_INITIAL_DAYS = 30


@dataclass
class Window:
    start: date
    end: date
    mode: str  # "incremental" or "full"


@dataclass
class RunResult:
    city_name: str
    run_id: int | None
    status: str
    rows: int = 0
    window: Window | None = None
    quality: QualityReport | None = None
    error: str | None = None


def yesterday_utc() -> date:
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def validate_dates(start: date | None, end: date | None) -> None:
    latest = yesterday_utc()
    if start is not None and start < EARLIEST_ARCHIVE_DATE:
        raise ValueError(f"Start date must be on or after {EARLIEST_ARCHIVE_DATE}.")
    if (end or start) is not None and (end or start) > latest:
        raise ValueError("Dates must be before today (UTC).")
    if start is not None and end is not None and start > end:
        raise ValueError("Start date must not be after end date.")


def plan_window(loaded_min: datetime | None, loaded_max: datetime | None,
                provisional_from: datetime | None,
                requested_start: date | None, requested_end: date | None, *,
                full_refresh: bool = False, initial_days: int = DEFAULT_INITIAL_DAYS,
                today_end: date | None = None) -> Window | None:
    """Return the date range that actually needs extracting, or None if up to date.

    Incremental runs resume after the latest stored hour, reaching back to the
    earliest provisional hour so provider revisions are picked up. Stored data
    is only skipped from the requested start: if the request begins before the
    earliest stored hour, the whole range is loaded so that history isn't lost.
    """
    end = requested_end or today_end or yesterday_utc()
    if full_refresh or loaded_max is None:
        # The default history only applies to a city with nothing stored yet.
        return Window(requested_start or (end - timedelta(days=initial_days - 1)), end, "full")
    if requested_start is not None and loaded_min.date() > requested_start:
        return Window(requested_start, end, "full")
    resume = (loaded_max + timedelta(hours=1)).date()
    if provisional_from is not None:
        resume = min(resume, provisional_from.date())
    # Without a requested start, resume exactly where stored data ends, however
    # long ago that was, so a city that missed several runs gets no gap.
    start = max(requested_start, resume) if requested_start else resume
    if start > end:
        return None
    return Window(start, end, "incremental")


def run_city(conn: psycopg.Connection, session: requests.Session, location: dict,
             start: date | None = None, end: date | None = None, *,
             full_refresh: bool = False,
             lookback_days: int = DEFAULT_LOOKBACK_DAYS,
             initial_days: int = DEFAULT_INITIAL_DAYS) -> RunResult:
    """Run one city end to end. Never raises for data/API/DB errors: the failure
    is logged, recorded in pipeline_runs, and returned so batches can continue."""
    name = location["name"]
    run_id = None
    window = None
    quality = None
    try:
        validate_dates(start, end)
        city_id = load.upsert_city(conn, location)
        loaded_min, loaded_max, provisional_from = load.loaded_state(conn, city_id, lookback_days)
        window = plan_window(loaded_min, loaded_max, provisional_from, start, end,
                             full_refresh=full_refresh, initial_days=initial_days)
        if window is None:
            run_id = load.start_run(conn, city_id, "incremental", start, end)
            load.finish_run(conn, run_id, "skipped")
            log.info("%s: already loaded through %s; nothing to extract (run_id=%s).",
                     name, loaded_max, run_id)
            return RunResult(name, run_id, "skipped")

        run_id = load.start_run(conn, city_id, window.mode, window.start, window.end)
        log.info("%s: %s load %s -> %s (run_id=%s).", name, window.mode,
                 window.start, window.end, run_id)
        raw = fetch_weather(session, location["latitude"], location["longitude"],
                            window.start, window.end)
        result = transform_weather(raw, window.start, window.end)
        quality = result.quality
        rows = load.load_observations(conn, city_id, result.frame)
        status = "warning" if quality.has_issues else "success"
        load.finish_run(conn, run_id, status, rows, quality)
        _log_quality(name, quality)
        log.info("%s: committed %s hourly rows (status=%s, run_id=%s).",
                 name, f"{rows:,}", status, run_id)
        return RunResult(name, run_id, status, rows, window, quality)
    except (requests.RequestException, psycopg.Error, ValueError, KeyError, TypeError) as exc:
        message = safe_error(exc)
        log.error("%s: failed: %s", name, message)
        if run_id is not None:
            try:
                load.finish_run(conn, run_id, "failed", quality=quality, error_message=message)
            except psycopg.Error:
                log.exception("%s: could not record failure for run_id=%s", name, run_id)
        return RunResult(name, run_id, "failed", window=window, quality=quality, error=message)


def safe_error(exc: Exception) -> str:
    # Avoid surfacing database exception details, which can expose credentials.
    if isinstance(exc, psycopg.Error):
        return (f"PostgreSQL operation failed (SQLSTATE {exc.sqlstate or 'unavailable'}). "
                "Check connection, permissions, and existing schema.")
    return str(exc) or exc.__class__.__name__


def _log_quality(name: str, quality: QualityReport) -> None:
    if quality.missing_hours:
        log.warning("%s: %d requested hours missing from the API response (first: %s).",
                    name, quality.missing_hours,
                    ", ".join(t.isoformat() for t in quality.first_missing))
    if quality.unavailable_hours:
        log.warning("%s: %d hours had no usable measurements and were not loaded "
                    "(often not yet published; a later incremental run will retry them).",
                    name, quality.unavailable_hours)
    for label, counts in (("null", quality.null_counts), ("invalid", quality.invalid_counts)):
        nonzero = {k: v for k, v in counts.items() if v}
        if nonzero:
            log.warning("%s: %s values by column: %s", name, label, nonzero)
