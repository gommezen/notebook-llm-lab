import argparse
from pathlib import Path

from notebook_llm_lab.ingestion.fit_reader import load_fit_dir

# import pandas as pd


def main():
    parser = argparse.ArgumentParser(
        description="Batch-convert .fit/.fit.gz to cleaned Parquet/CSV"
    )
    parser.add_argument("--input", "-i", type=str, default="data/strava/raw")
    parser.add_argument("--outdir", "-o", type=str, default="data/strava/processed")
    args = parser.parse_args()

    input_dir = Path(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Load and clean all FIT files
    df = load_fit_dir(input_dir)
    if df.empty:
        print(f"No .fit/.fit.gz files found in {input_dir}")
        return

    parquet_path = outdir / "strava_runs.parquet"
    csv_path = outdir / "strava_runs.csv"

    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)
    print(f"✅ Saved {len(df):,} records to:\n- {parquet_path}\n- {csv_path}")


if __name__ == "__main__":
    main()
