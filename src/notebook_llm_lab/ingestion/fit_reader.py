#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Convert Strava .fit/.fit.gz files to a cleaned pandas DataFrame.

This module provides helpers to read Garmin/Strava FIT activity files (single
or gzipped) and normalize the resulting records into a DataFrame with a few
derived convenience columns (distance_km, pace_min_per_km, lat/lon).

The reader is defensive — it supports multiple FIT libraries and message
shapes by trying `get_values()`, `fields` (dict or iterable), and a safe
iterable fallback.

Usage:
    from notebook_llm_lab.ingestion.fit_reader import read_and_clean_fit, load_fit_dir
    df = load_fit_dir("data/strava/raw")

Requires:
    pandas and (optionally) fitparse. The code will log an error if a FIT
    reader library is not available.
"""

import logging
import gzip
import io
from pathlib import Path
from typing import Dict, Any
import pandas as pd

try:
    from fitparse import FitFile  # optional dependency; used when available
except Exception:  # pragma: no cover - graceful degradation when fitparse isn't installed
    FitFile = None  # type: ignore

SEMICIRCLE_TO_DEG = 180 / (2**31)

logger = logging.getLogger(__name__)


def _extract_fields_from_message(record) -> Dict[str, Any]:
    """Return a mapping of field-name -> value for a FIT message.

    Tries, in order:
    1. record.get_values() -> dict
    2. record.fields as a dict or iterable of field objects (.name/.value)
    3. fallback to iterating `record` (some parsers yield field objects)
    """
    data: Dict[str, Any] = {}

    # 1) Prefer a direct dict if available
    get_values = getattr(record, "get_values", None)
    if callable(get_values):
        try:
            vals = get_values()
            if isinstance(vals, dict):
                for k, v in vals.items():
                    if v is not None:
                        data[k] = v
                if data:
                    return data
        except Exception:
            logger.debug("record.get_values() raised, falling back", exc_info=True)

    # 2) record.fields may be a dict or an iterable of field objects
    fields = getattr(record, "fields", None)
    if fields is not None:
        try:
            if isinstance(fields, dict):
                for k, v in fields.items():
                    if v is None:
                        continue
                    if hasattr(v, "value"):
                        val = getattr(v, "value", None)
                        name = getattr(v, "name", k)
                        if val is not None:
                            data[name] = val
                    else:
                        data[k] = v
            else:
                for f in fields:
                    name = getattr(f, "name", None)
                    value = getattr(f, "value", None)
                    if name and value is not None:
                        data[name] = value
            if data:
                return data
        except Exception:
            logger.debug("record.fields parsing failed, falling back", exc_info=True)

    # 3) Fallback: some message objects are iterable and yield field objects
    try:
        for f in record:
            name = getattr(f, "name", None)
            value = getattr(f, "value", None)
            if name and value is not None:
                data[name] = value
        return data
    except Exception:
        return {}

def read_fit(path: Path) -> pd.DataFrame:
    """Read a single .fit or .fit.gz file and return a DataFrame of record data."""
    path = Path(path)
    if FitFile is None:
        logger.error("FIT reader library (fitparse) is not installed; cannot read FIT files.")
        return pd.DataFrame()

    try:
        # Handle .fit.gz by decompressing to memory
        if path.suffix == ".gz":
            import gzip, io
            with gzip.open(path, "rb") as f:
                file_bytes = f.read()
            fitfile = FitFile(io.BytesIO(file_bytes))
        else:
            fitfile = FitFile(str(path))

        rows = []  # <- list of dicts (one per record)
        for record in fitfile.get_messages("record"):
            if record is None:
                continue
            try:
                fields = _extract_fields_from_message(record)
                if fields:
                    rows.append(fields)
            except Exception as e:
                logger.debug("Skipping bad record in %s: %s", path.name, e, exc_info=True)
                continue

        if not rows:
            logger.warning("No record data in %s", path.name)
            return pd.DataFrame()

        df = pd.DataFrame.from_records(rows)  # robust to varying keys/lengths
        return df

    except Exception as e:
        logger.exception("Failed to read %s: %s", path.name, e)
        return pd.DataFrame()


def _add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add convenience columns and normalize units."""
    if df.empty:
        return df.copy()

    df = df.copy()

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    if "distance" in df.columns:
        df["distance_km"] = df["distance"] / 1000.0

    if "speed" in df.columns:
        df["pace_min_per_km"] = df["speed"].apply(
            lambda v: (1000.0 / v) / 60.0 if isinstance(v, (int, float)) and v > 0 else None
        )

    if "enhanced_altitude" in df.columns and "altitude" not in df.columns:
        df["altitude"] = df["enhanced_altitude"]

    if "position_lat" in df.columns:
        df["lat"] = df["position_lat"] * SEMICIRCLE_TO_DEG
    if "position_long" in df.columns:
        df["lon"] = df["position_long"] * SEMICIRCLE_TO_DEG

    if "timestamp" in df.columns:
        df = (
            df.dropna(subset=["timestamp"])
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

    return df


def read_and_clean_fit(path: Path) -> pd.DataFrame:
    """Load and clean a single FIT file."""
    raw = read_fit(path)
    return _add_derived_columns(raw)


def load_fit_dir(dirpath: Path | str) -> pd.DataFrame:
    """Load all .fit/.fit.gz files in a directory into one cleaned DataFrame."""
    dirpath = Path(dirpath)
    all_files = sorted([*dirpath.glob("*.fit"), *dirpath.glob("*.fit.gz")])
    frames = []

    for p in all_files:
        df = read_and_clean_fit(p)
        if df.empty:
            logger.warning("Skipping empty or unreadable file: %s", p.name)
            continue
        df["run_id"] = p.stem
        frames.append(df)

    if not frames:
        logger.warning("No valid FIT files found in %s", dirpath)
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)
