#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convert Strava .fit/.fit.gz files to cleaned CSV and Parquet.

This script scans an input folder for FIT activity files, parses and cleans them
(using src.ingestion.fit_reader), and writes the combined result to:
    data/strava/processed/strava_runs.{csv,parquet}

Usage:
    python scripts/fit_to_parquet.py [-i INPUT_DIR] [-o OUTPUT_DIR]

Requires:
    pandas, pyarrow, and fitparse (or fitfile fork).
"""

from __future__ import annotations
from pathlib import Path
import argparse
import pandas as pd
from notebook_llm_lab.ingestion.fit_reader import load_fit_dir


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Batch-convert .fit/.fit.gz to cleaned Parquet/CSV"
    )
    parser.add_argument("--input", "-i", type=str, default="data/strava/raw")
    parser.add_argument("--outdir", "-o", type=str, default="data/strava/processed")
    args = parser.parse_args()

    input_dir = Path(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df = load_fit_dir(input_dir)

    if df.empty:
        print(f"No .fit/.fit.gz files found in {input_dir}")
        return

    parquet_path = outdir / "strava_runs.parquet"
    csv_path = outdir / "strava_runs.csv"

    # Optional: ensure columns exist before saving
    # (Prevents writing empty structure if something went wrong)
    if df.shape[1] == 0:
        print("Dataframe has no columns. Aborting save.")
        return

    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)

    print(f"✅ Saved {len(df):,} records to:\n- {parquet_path}\n- {csv_path}")


if __name__ == "__main__":
    main()
