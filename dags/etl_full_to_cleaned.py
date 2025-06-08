"""
DAG: etl_full_to_cleaned

Description:
    This Airflow DAG manages the ETL process from the raw (landing) layer to the cleaned and archive_cleaned layers. It ensures data quality and prepares data for downstream analytical processing.

Scheduling:
    [PLACEHOLDER: Specify scheduling requirements here. Currently, schedule_interval=None, so this DAG must be triggered manually.]

Author:
    ETL Team
    University Project – GreenPals / Company IoT
"""

# etl_full_to_cleaned.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "etl_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="raw_to_cleaned_and_archive_cleaned",
    default_args=default_args,
    description="Od LANDING → CLEANED → ARCHIVE_CLEANED",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["silver","cleaned"],
) as dag:

    # zamijeni BashOperator za populate_cleaned_python.py
    run_populate_cleaned = PythonOperator(
        task_id="run_full_load_cleaned",
        python_callable=lambda: __import__("run_full_cleaned_load").main(),
    )

    run_scd2_cleaned = PostgresOperator(
        task_id="run_scd2_cleaned_sql",
        postgres_conn_id="postgres_analytical",
        sql="scd2_cleaned.sql",
        autocommit=True,
    )

    run_populate_cleaned >> run_scd2_cleaned
