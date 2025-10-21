from __future__ import annotations
import gzip
from pathlib import Path
from typing import Iterable, List, Dict
import pandas as pd

# Prefer fitparse; fallback to fitfile
try:
    from fitparse import FitFile  # type: ignore  # fitparse is the official library
    USING_FITPARSE = True
except Exception:
    from fitfile import FitFile  # type: ignore  # unofficial fork; API may differ
    USING_FITPARSE = False


SEMICIRCLE_TO_DEG = 180 / (2**31)


def _iter_fit_records(fit: FitFile) -> Iterable[Dict]:
    """
    Yield a dict of field_name -> value for each "record" message.
    Supports both fitparse (preferred) and older fitfile forks.
    """
    # --- Preferred path: fitparse (clean API) ---
    if USING_FITPARSE and hasattr(fit, "get_messages"):
        for msg in fit.get_messages("record"):  # type: ignore[attr-defined]
            row = {}
            # msg is iterable at runtime (fitparse defines __iter__), but not typed
            for field in msg:  # type: ignore
                # field.name and field.value are documented in fitparse
                row[field.name] = field.value
            yield row
        return

    # --- Fallback path: older fitfile or forks ---
    #  They may define `.get_messages`, `.records`, or `.messages`.
    if hasattr(fit, "get_messages"):
        messages = fit.get_messages("record")  # type: ignore[attr-defined]
    elif hasattr(fit, "records"):
        messages = getattr(fit, "records")  # type: ignore[attr-defined]
    elif hasattr(fit, "messages"):
        # Filter only record-type messages
        messages = [
            m for m in getattr(fit, "messages")  # type: ignore[attr-defined]
            if getattr(m, "name", "") == "record"
        ]
    else:
        raise RuntimeError(
            "Unsupported FITFile API. Please install 'fitparse' for full functionality."
        )

    # Iterate over found messages
    for msg in messages:
        row = {}
        # Some forks allow direct iteration over msg fields
        if hasattr(msg, "__iter__"):
            for field in msg:  # type: ignore
                name = getattr(field, "name", getattr(field, "field_name", None))
                value = getattr(field, "value", None)
                if name is not None:
                    row[name] = value
        yield row


def _read_fit_bytes(fp) -> pd.DataFrame:
    fit = FitFile(fp)  # type: ignore[call-arg]
    records = list(_iter_fit_records(fit))
    return pd.DataFrame.from_records(records)


def read_fit(path: Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix == ".gz":
        with gzip.open(path, "rb") as f:
            return _read_fit_bytes(f)
    with open(path, "rb") as f:
        return _read_fit_bytes(f)


def _add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Timestamp
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Distance -> km
    if "distance" in df.columns:
        df["distance_km"] = df["distance"] / 1000.0

    # Speed -> pace
    if "speed" in df.columns:
        df["pace_min_per_km"] = df["speed"].apply(
            lambda v: (1000.0 / v) / 60.0 if v and v > 0 else None
        )

    # Altitude fix
    if "enhanced_altitude" in df.columns and "altitude" not in df.columns:
        df["altitude"] = df["enhanced_altitude"]

    # Lat/Lon conversion
    if "position_lat" in df.columns:
        df["lat"] = df["position_lat"] * SEMICIRCLE_TO_DEG
    if "position_long" in df.columns:
        df["lon"] = df["position_long"] * SEMICIRCLE_TO_DEG

    # Sort by time
    if "timestamp" in df.columns:
        df = (
            df.dropna(subset=["timestamp"])
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

    return df


def read_and_clean_fit(path: Path) -> pd.DataFrame:
    raw = read_fit(path)
    return _add_derived_columns(raw)


def load_fit_dir(dirpath: Path | str) -> pd.DataFrame:
    """
    Load and clean all .fit or .fit.gz files in a directory.
    Each file is processed independently to avoid crashing on bad files.
    """
    dirpath = Path(dirpath)
    all_files = sorted([*dirpath.glob("*.fit"), *dirpath.glob("*.fit.gz")])

    frames = []
    failed = []
    
    # Process files one by one
    for p in all_files:
        try:
            df = read_and_clean_fit(p)
            run_id = p.stem.replace(".fit", "")
            df["run_id"] = run_id
            frames.append(df)
        except Exception as e:
            # Store (filename, error message)
            failed.append((p.name, str(e)))

    # Print result summary
    print(f"✅ Successfully loaded {len(frames)}/{len(all_files)} files.")

    if failed:
        print("⚠️ Failed files:")
        for name, err in failed:
            print(f"  - {name}: {err}")

    # If no files loaded successfully, return empty DataFrame
    if not frames:
        return pd.DataFrame()

    # Concatenate the loaded DataFrames
    return pd.concat(frames, ignore_index=True)
