"""Transform: validate an archive response and shape it into fact rows.

Two kinds of problems are handled differently:

* Structural problems (wrong units, mismatched arrays, duplicate or
  out-of-window timestamps) mean the response itself cannot be trusted, so
  the whole run fails.
* Individual bad or missing measurements are set to NULL and counted. One
  missing wind reading no longer discards the other 719 hours of a backfill.

Gaps in the hourly series are detected and reported, never filled in.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta

import numpy as np
import pandas as pd

from src.extract import VARIABLES

MEASURES = list(VARIABLES.values())
EXPECTED_UNITS = {"temperature_2m": "°C", "precipitation": "mm",
                  "relative_humidity_2m": "%", "wind_speed_10m": "km/h",
                  "wind_direction_10m": "°"}
# Inclusive valid ranges; None means unbounded on that side.
VALID_RANGES = {
    "temperature_c": (-100, 70),
    "precipitation_mm": (0, None),
    "relative_humidity_pct": (0, 100),
    "wind_speed_kmh": (0, None),
    "wind_direction_deg": (0, 360),
}


@dataclass
class QualityReport:
    expected_hours: int
    missing_hours: int = 0        # requested hours absent from the response
    unavailable_hours: int = 0    # hours present but with no usable measurement
    null_counts: dict[str, int] = field(default_factory=dict)
    invalid_counts: dict[str, int] = field(default_factory=dict)
    first_missing: list[pd.Timestamp] = field(default_factory=list)

    @property
    def has_issues(self) -> bool:
        return bool(self.missing_hours or self.unavailable_hours
                    or any(self.null_counts.values()) or any(self.invalid_counts.values()))


@dataclass
class TransformResult:
    frame: pd.DataFrame
    quality: QualityReport


def expected_hours(start: date, end: date) -> pd.DatetimeIndex:
    """Every UTC hour in the inclusive date range."""
    return pd.date_range(pd.Timestamp(start, tz="UTC"),
                         pd.Timestamp(end + timedelta(days=1), tz="UTC"),
                         freq="h", inclusive="left")


def transform_weather(raw: dict, start: date, end: date) -> TransformResult:
    _check_structure(raw)
    hourly = raw["hourly"]
    frame = pd.DataFrame({k: hourly[k] for k in ["time", *VARIABLES]})
    frame = frame.rename(columns={"time": "observed_at", **VARIABLES})
    frame["observed_at"] = pd.to_datetime(frame["observed_at"], utc=True, errors="raise")
    frame = frame.sort_values("observed_at").reset_index(drop=True)

    expected = expected_hours(start, end)
    observed = pd.DatetimeIndex(frame["observed_at"])
    if observed.has_duplicates:
        raise ValueError("API returned duplicate timestamps.")
    if not observed.isin(expected).all():
        raise ValueError("API returned timestamps outside the requested UTC hours.")
    missing = expected.difference(observed)
    quality = QualityReport(expected_hours=len(expected), missing_hours=len(missing),
                            first_missing=list(missing[:5]))

    for column in MEASURES:
        original = frame[column]
        values = pd.to_numeric(original, errors="coerce").astype("float64")
        values = values.where(np.isfinite(values))
        low, high = VALID_RANGES[column]
        out_of_range = pd.Series(False, index=values.index)
        if low is not None:
            out_of_range |= values < low
        if high is not None:
            out_of_range |= values > high
        values = values.mask(out_of_range)
        # Present in the response but unusable: non-numeric, infinite, or out of range.
        quality.invalid_counts[column] = int((original.notna() & values.isna()).sum())
        frame[column] = values

    # Hours with no usable measurement at all are usually not yet published
    # (the archive lags real time by several days). Skip them so a later
    # incremental run, via its lookback window, can load them once available.
    empty = frame[MEASURES].isna().all(axis=1)
    quality.unavailable_hours = int(empty.sum())
    frame = frame.loc[~empty].reset_index(drop=True)
    quality.null_counts = {c: int(frame[c].isna().sum()) for c in MEASURES}
    return TransformResult(frame=frame, quality=quality)


def _check_structure(raw: dict) -> None:
    if raw.get("utc_offset_seconds") != 0:
        raise ValueError("Expected UTC archive data.")
    hourly = raw.get("hourly")
    fields = ["time", *VARIABLES]
    if not isinstance(hourly, dict) or any(not isinstance(hourly.get(k), list) for k in fields):
        raise ValueError("API response lacks required hourly arrays.")
    if len({len(hourly[k]) for k in fields}) != 1:
        raise ValueError("Hourly arrays have different lengths.")
    units = raw.get("hourly_units", {})
    if any(units.get(k) != v for k, v in EXPECTED_UNITS.items()):
        raise ValueError("Unexpected weather units.")
