"""
Airflow DAG to orchestrate the ETL pipeline. Place this file in your Airflow `dags/` folder.
It imports the scripts from the repository root using a small sys.path hack so Airflow can find them.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
import sys
from pathlib import Path
import polars as pl

# add project root to path so DAG can import scripts

PROJECT_ROOT = Path(__file__).resolve().parents[1]/"Retail_etl_project"
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.extract import extract_all
from scripts.transform import clean_and_transform, write_analytics
from scripts.load import load_to_parquet


def extract_task_func(**kwargs):
    df = extract_all("sales_*.csv")
    out_path = PROJECT_ROOT / "Data" / "processed" / "extracted.parquet"
    out_path.parent.mkdir(parents=True,exist_ok=True)
    df.write_parquet(out_path)
    return str(out_path)


def transform_task_func(**kwargs):
    processed = PROJECT_ROOT / "Data" / "processed"
    extracted = processed / "extracted.parquet"

    if not extracted.exists():
        raise FileNotFoundError("extracted.parquet not found. Did extract run?")

    df = pl.read_parquet(extracted)
    df_clean = clean_and_transform(df)
    df_clean.write_parquet(processed / "clean_sales.parquet")
    write_analytics(df_clean)


def load_task_func(**kwargs):
    load_to_parquet()


default_args = {
    "start_date": datetime.now() - timedelta(days=1)
}

with DAG(
    dag_id="retail_sales_etl",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_task_func,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_task_func,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_task_func,
    )

    extract_task >> transform_task >> load_task
