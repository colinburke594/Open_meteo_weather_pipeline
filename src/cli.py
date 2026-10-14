"""Interactive prompts. The only module that calls input()."""
from __future__ import annotations

from datetime import date

import requests

from src.geocoding import describe_location, search_locations
from src.pipeline import validate_dates


def prompt_location(session: requests.Session) -> dict:
    while True:
        city = input("City (e.g. San Jose): ")
        state = input("State/province, full name (optional): ")
        country = input("Country name or 2-letter code (optional): ")
        try:
            results = search_locations(session, city, state, country)
        except ValueError as exc:
            print(exc)
            continue
        if not results:
            print("No matching populated places. Try full state/country names or a country code.")
            continue
        location = select_location(results)
        if location is not None:
            return location


def select_location(locations: list[dict]) -> dict | None:
    """Ask the user to pick one result; None means search again."""
    for number, location in enumerate(locations, 1):
        print(f"{number}. {describe_location(location)}")
    print("Results are limited to 100; refine the search if your city is absent.")
    while True:
        choice = input("Select a number, or 0 to search again: ").strip()
        if choice == "0":
            return None
        if choice.isdigit() and 1 <= int(choice) <= len(locations):
            return locations[int(choice) - 1]
        print("Enter one of the displayed numbers.")


def prompt_dates() -> tuple[date | None, date | None]:
    """Blank answers mean: continue incrementally from the last stored hour."""
    print("Leave dates blank to load only what is new since the last run.")
    while True:
        try:
            start = _parse_optional(input("Start date (YYYY-MM-DD, UTC, optional): "))
            end = _parse_optional(input("End date (YYYY-MM-DD, inclusive UTC, optional): "))
            validate_dates(start, end)
            return start, end
        except ValueError as exc:
            print(f"Invalid dates: {exc}")


def _parse_optional(text: str) -> date | None:
    text = text.strip()
    return date.fromisoformat(text) if text else None
