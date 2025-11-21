"""
load.py

Simple loader demonstrating writing cleaned data to target storage (Parquet/CSV).
In production this could push to a database or S3.
"""
import polars as pl
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = ROOT / "Data" / "processed"


def load_to_parquet():
    src = PROCESSED_DIR / "clean_sales.parquet"
    if not src.exists():
        print("No cleaned parquet found. Run transform.py first.")
        return
    df = pl.read_parquet(src)
    out = PROCESSED_DIR / "clean_sales_copy.parquet"
    df.write_parquet(out)
    print(f"Wrote {out}")
    return df


if __name__ == "__main__":
    load_to_parquet()
