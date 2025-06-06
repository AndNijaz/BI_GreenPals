# etl_full_to_cleaned.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "etl_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="raw_to_cleaned_and_archive_cleaned",
    default_args=default_args,
    description="Od LANDING → ARCHIVE_RAW (već radi), pa ARCHIVE_RAW → CLEANED → ARCHIVE_CLEANED",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # ili cron po potrebi
    catchup=False,
    tags=["silver", "cleaned", "scd2_cleaned"],
) as dag:

    # 1) Pretpostavka: LANDING i ARCHIVE_RAW su već napunjeni (full/incremental)
    #    Ako trebate napraviti full load prije, ubacite to kao prethodni task.

    # 2) Napuni CLEANED iz ARCHIVE_RAW
    run_populate_cleaned = BashOperator(
        task_id="run_populate_cleaned_py",
        bash_command="python populate_cleaned_python.py",
        cwd="/opt/airflow/dags",
    )

    # 3) Pokreni SCD2_CLEANED skriptu
    run_scd2_cleaned = PostgresOperator(
        task_id="run_scd2_cleaned_sql",
        postgres_conn_id="postgres_analytical",
        sql="scd2_cleaned.sql",
        autocommit=True,
    )

    run_populate_cleaned >> run_scd2_cleaned
