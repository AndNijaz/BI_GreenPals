"""
DAG: populate_cleaned

Description:
    This Airflow DAG populates the CLEANED layer from ARCHIVE_RAW, ensuring that the cleaned tables are up-to-date and ready for further processing or analysis.

Scheduling:
    [PLACEHOLDER: Specify scheduling requirements here. Currently, schedule_interval=None, so this DAG must be triggered manually.]

Author:
    ETL Team
    University Project – GreenPals / Company IoT
"""

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
    dag_id="populate_cleaned",
    default_args=default_args,
    description="Popuni CLEANED iz ARCHIVE_RAW",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # ručno, ili cron po potrebi
    catchup=False,
    tags=["silver", "cleaned"],
) as dag:

    # Step 1: Pokreni Python skriptu koja puni cleaned.* 
    run_populate_cleaned = BashOperator(
        task_id="run_populate_cleaned_py",
        bash_command="python populate_cleaned_python.py",
        cwd="/opt/airflow/dags",
    )

    #  (Eventualno) Step 2: Možete odmah nakon toga pokrenuti ETL za archive_cleaned (SCD2_CLEANED)
    run_scd2_cleaned = PostgresOperator(
        task_id="run_scd2_cleaned_sql",
        postgres_conn_id="postgres_analytical",
        sql="scd2_cleaned.sql",
        autocommit=True,
    )

    run_populate_cleaned >> run_scd2_cleaned
