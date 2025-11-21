"""
transform.py

Load extracted parquet (or raw CSVs), clean data, add computed columns, and write analytical CSVs.
"""
import polars as pl
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = ROOT / "Data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def clean_and_transform(df: pl.DataFrame) -> pl.DataFrame:
    # Basic cleaning
    df = (
        df.with_columns([
            pl.col("order_id").cast(pl.Int64),
            pl.col("customer").str.strip_chars(),
            pl.col("price").cast(pl.Float64),
            pl.col("quantity").cast(pl.Int64),
            pl.col("date").str.strptime(pl.Date)
        ])
    )

    # Remove rows without order_id
    df = df.filter(pl.col("order_id").is_not_null())

    # Replace empty customer with None
    df = df.with_columns(pl.when(pl.col("customer").is_null() | (pl.col("customer") == "")).then(None).otherwise(pl.col("customer")).alias("customer"))

    # Computed column
    df = df.with_columns((pl.col("quantity") * pl.col("price")).alias("total_amount")) 

    # Standardize product names
    df = df.with_columns(pl.col("product").str.to_lowercase().str.strip_chars())

    return df


def write_analytics(df: pl.DataFrame):
    # Using DuckDB for SQL analytics on the Polars DataFrame
    con = duckdb.connect(database=':memory:')
    con.register('sales', df.to_pandas())  # DuckDB works well with pandas

    # Revenue by product
    revenue_by_product = con.execute('''
        SELECT product, SUM(total_amount) AS total_revenue
        FROM sales
        GROUP BY product
        ORDER BY total_revenue DESC
    ''').fetchdf()

    # Customer summary
    customer_summary = con.execute('''
        SELECT customer, COUNT(*) AS total_orders, SUM(total_amount) AS total_spent
        FROM sales
        GROUP BY customer
        ORDER BY total_spent DESC
    ''').fetchdf()

    # Daily sales
    daily_sales = con.execute('''
        SELECT date, SUM(total_amount) AS daily_sales
        FROM sales
        GROUP BY date
        ORDER BY date
    ''').fetchdf()

    # Save outputs
    pl.from_pandas(revenue_by_product).write_csv(PROCESSED_DIR / "revenue_by_product.csv")
    pl.from_pandas(customer_summary).write_csv(PROCESSED_DIR / "customer_summary.csv")
    pl.from_pandas(daily_sales).write_csv(PROCESSED_DIR / "daily_sales.csv")

    print("Analytics CSVs written to data/processed/")


if __name__ == "__main__":
    # If extracted parquet exists, load it; otherwise read raw
    extracted_path = PROCESSED_DIR / "extracted.parquet"
    if extracted_path.exists():
        df = pl.read_parquet(extracted_path)
    else:
        raw = ROOT / "data" / "raw" / "sample_sales.csv"
        df = pl.read_csv(raw)

    df_clean = clean_and_transform(df)
    # save cleaned parquet
    df_clean.write_parquet(PROCESSED_DIR / "clean_sales.parquet")
    write_analytics(df_clean)
