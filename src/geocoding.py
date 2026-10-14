"""Geocoding: search the Open-Meteo Geocoding API. No user interaction here.

Choosing among several matches is the caller's job: the CLI asks the user
(src/cli.py), batch mode uses an explicit geocoding_id or the top-ranked match.
"""
from __future__ import annotations

import requests

from src.extract import get_json

GEOCODING_SEARCH_URL = "https://geocoding-api.open-meteo.com/v1/search"
GEOCODING_GET_URL = "https://geocoding-api.open-meteo.com/v1/get"


def search_locations(session: requests.Session, city: str, state: str = "",
                     country: str = "") -> list[dict]:
    """Return populated places matching the query, in the provider's ranking order."""
    city, state, country = city.strip(), state.strip(), country.strip()
    if len(city) < 2 or "," in city or "," in state:
        raise ValueError("City must be at least two characters, and city/state must not contain commas.")
    # The API accepts one qualifier; use state there and a separate country
    # code filter, or check the full country name on returned candidates.
    qualifier = state or (country if len(country) != 2 else "")
    params = {"name": city + (f", {qualifier}" if qualifier else ""),
              "count": 100, "language": "en", "format": "json"}
    if len(country) == 2:
        params["countryCode"] = country.upper()
    results = get_json(session, GEOCODING_SEARCH_URL, params).get("results", [])
    return filter_locations(results, country)


def filter_locations(results: list[dict], country: str = "") -> list[dict]:
    """Keep populated places, apply a full country-name filter, drop duplicate IDs."""
    if country and len(country) != 2:
        results = [r for r in results if r.get("country", "").casefold() == country.casefold()]
    # The archive pipeline represents populated places, not mountain peaks.
    results = [r for r in results if r.get("feature_code", "").startswith("PPL")]
    return [validate_location(r) for r in {r["id"]: r for r in results}.values()]


def get_location(session: requests.Session, geocoding_id: int) -> dict:
    """Look up one location by its stable provider ID (unambiguous, for batch runs)."""
    return validate_location(get_json(session, GEOCODING_GET_URL, {"id": int(geocoding_id)}))


def validate_location(location: dict) -> dict:
    if "id" not in location or "name" not in location:
        raise ValueError("Geocoding result lacks an id or name.")
    lat, lon = float(location["latitude"]), float(location["longitude"])
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise ValueError(f"Geocoding returned invalid coordinates for {location['name']}.")
    return location


def describe_location(location: dict) -> str:
    region = ", ".join(str(location[k]) for k in ("name", "admin1", "admin2", "country")
                       if location.get(k))
    return (f"{region} [{location.get('country_code', '?')}] | "
            f"{location['latitude']}, {location['longitude']} | ID {location['id']} | "
            f"timezone {location.get('timezone', 'unknown')}")
