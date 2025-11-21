"""
extract.py


Reads CSV files from data/raw/ and writes a combined parquet to data/processed/ for downstream steps.
"""
import polars as pl
from pathlib import Path


RAW_DIR = Path(__file__).resolve().parents[1] / "Data" / "raw"
PROCESSED_DIR = Path(__file__).resolve().parents[1] / "Data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)




def extract_all(pattern: str = "sales_*.csv") -> pl.DataFrame:
    files = sorted(RAW_DIR.glob(pattern))
    if not files:
         # fallback to sample file
        files = [RAW_DIR / "sample_sales.csv"]
        # Polars can accept   a list of files directly
        df_lazy = pl.scan_csv([str(f) for f in files])
        df = df_lazy.collect()
        print(f"Extracted {df.shape[0]} rows from {len(files)} file(s)")
    return df
 
 
if __name__ == "__main__": 
    df = extract_all("sales_*.csv")
    out_path = PROCESSED_DIR / "extracted.parquet"
    df.write_parquet(out_path)
    print(f"Wrote extracted parquet: {out_path}")
