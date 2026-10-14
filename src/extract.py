"""Extract: HTTP access to the Open-Meteo archive API."""
from __future__ import annotations

from datetime import date

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# API variable name -> warehouse column name.
VARIABLES = {
    "temperature_2m": "temperature_c",
    "precipitation": "precipitation_mm",
    "relative_humidity_2m": "relative_humidity_pct",
    "wind_speed_10m": "wind_speed_kmh",
    "wind_direction_10m": "wind_direction_deg",
}


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"], respect_retry_after_header=True)
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def get_json(session: requests.Session, url: str, params: dict) -> dict:
    response = session.get(url, params=params, timeout=(10, 120))
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise ValueError("API returned an unexpected response.")
    if data.get("error"):
        raise ValueError(f"Open-Meteo: {data.get('reason', 'unknown API error')}")
    return data


def fetch_weather(session: requests.Session, latitude: float, longitude: float,
                  start: date, end: date) -> dict:
    return get_json(session, ARCHIVE_URL, {
        "latitude": latitude, "longitude": longitude,
        "start_date": start.isoformat(), "end_date": end.isoformat(),
        "hourly": ",".join(VARIABLES), "timezone": "UTC", "timeformat": "iso8601",
        "temperature_unit": "celsius", "precipitation_unit": "mm", "wind_speed_unit": "kmh",
    })
