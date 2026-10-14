#!/usr/bin/env python3
"""Open-Meteo archive -> pandas -> PostgreSQL ETL (Python 3.10+).

Modes:
  python main.py                        interactive: search for one city, pick dates
  python main.py --cities cities.csv    batch: load every city in a CSV, no prompts
  python main.py --refresh-all          batch: incremental update of every stored city

Without --start, each city loads only hours newer than its last stored hour,
plus recent hours that were still provisional when loaded (provider revisions). Dates are inclusive
UTC calendar dates. Archive values are modeled reanalysis, not necessarily
station measurements. Re-running replaces matching hours; it never duplicates.
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from datetime import date
from pathlib import Path

import psycopg
import requests
from dotenv import load_dotenv

from src import cli, load
from src.extract import make_session
from src.geocoding import describe_location, get_location, search_locations
from src.pipeline import (DEFAULT_INITIAL_DAYS, DEFAULT_LOOKBACK_DAYS, RunResult,
                          run_city, safe_error, validate_dates)

log = logging.getLogger("weather_pipeline")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--cities", type=Path, metavar="CSV",
                        help="CSV with columns city,state,country,geocoding_id (geocoding_id "
                             "is optional but makes the match unambiguous)")
    source.add_argument("--refresh-all", action="store_true",
                        help="update every city already in dim_city")
    parser.add_argument("--start", type=date.fromisoformat, help="start date, YYYY-MM-DD (UTC)")
    parser.add_argument("--end", type=date.fromisoformat,
                        help="inclusive end date, YYYY-MM-DD (UTC); default yesterday")
    parser.add_argument("--full-refresh", action="store_true",
                        help="ignore stored data and reload the whole requested range")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
                        help="rows loaded within this many days of their hour are treated as "
                             "provisional and re-read by later runs (default %(default)s)")
    parser.add_argument("--initial-days", type=int, default=DEFAULT_INITIAL_DAYS,
                        help="history loaded for a new city with no --start (default %(default)s)")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args(argv)


def resolve_batch_locations(session: requests.Session, path: Path) -> list[dict]:
    """Resolve every CSV row without prompting. Unresolvable rows are logged and skipped."""
    locations = []
    with path.open(newline="", encoding="utf-8") as handle:
        for line, row in enumerate(csv.DictReader(handle), start=2):
            row = {k.strip().lower(): (v or "").strip() for k, v in row.items() if k}
            try:
                if row.get("geocoding_id"):
                    location = get_location(session, int(row["geocoding_id"]))
                else:
                    matches = search_locations(session, row.get("city", ""),
                                               row.get("state", ""), row.get("country", ""))
                    if not matches:
                        raise ValueError("no matching populated place")
                    location = matches[0]
                    if len(matches) > 1:
                        log.warning("%s line %d: %d matches, using top-ranked %s. Add a "
                                    "geocoding_id column to pin the choice.",
                                    path.name, line, len(matches), describe_location(location))
                locations.append(location)
            except (requests.RequestException, ValueError, KeyError) as exc:
                log.error("%s line %d (%s): %s", path.name, line, row.get("city"), exc)
    return locations


def run(args: argparse.Namespace, database_url: str) -> int:
    validate_dates(args.start, args.end)
    with make_session() as session, load.connect(database_url) as conn:
        load.ensure_schema(conn)
        start, end = args.start, args.end
        if args.refresh_all:
            locations = load.list_cities(conn)
        elif args.cities:
            locations = resolve_batch_locations(session, args.cities)
        else:
            locations = [cli.prompt_location(session)]
            if start is None and end is None:
                start, end = cli.prompt_dates()
        if not locations:
            log.error("No cities to process.")
            return 1

        results = [run_city(conn, session, location, start, end,
                            full_refresh=args.full_refresh,
                            lookback_days=args.lookback_days,
                            initial_days=args.initial_days)
                   for location in locations]
    print_summary(results)
    return 1 if any(r.status == "failed" for r in results) else 0


def print_summary(results: list[RunResult]) -> None:
    if len(results) > 1:
        print(f"\n{'city':<24} {'status':<9} {'rows':>8}  run_id")
        for r in results:
            print(f"{r.city_name[:24]:<24} {r.status:<9} {r.rows:>8,}  {r.run_id}")
    counts = {s: sum(r.status == s for r in results) for s in ("success", "warning", "skipped", "failed")}
    print("Done: " + ", ".join(f"{n} {s}" for s, n in counts.items() if n)
          + ". Details in public.pipeline_runs.")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=args.log_level,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    load_dotenv(Path(__file__).with_name(".env"))
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("Set DATABASE_URL in the environment or .env beside this script.", file=sys.stderr)
        return 1
    try:
        return run(args, database_url)
    except (KeyboardInterrupt, EOFError):
        print("\nCancelled.", file=sys.stderr)
        return 130
    except (requests.RequestException, psycopg.Error, OSError, ValueError) as exc:
        log.error("Pipeline failed: %s", safe_error(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
