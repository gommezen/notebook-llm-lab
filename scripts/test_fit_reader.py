#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
from notebook_llm_lab.ingestion.fit_reader import load_fit_dir



def main():
    df = load_fit_dir("data/strava/raw")
    print(df.shape)
    print(df.head())

if __name__ == "__main__":
    main()

