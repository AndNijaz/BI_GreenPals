"""
DAG: incremental_scd2_etl

Description:
    This Airflow DAG executes an incremental ETL process, loading new or updated records from the operational database into the analytical landing layer, followed by SCD2 updates into the archive layer. This supports near real-time data warehousing and historical tracking.

Scheduling:
    [PLACEHOLDER: Specify scheduling requirements here. Currently, schedule_interval=None, so this DAG must be triggered manually.]

Author:
    ETL Team
    University Project – GreenPals / Company IoT
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "owner": "etl_team",
}

with DAG(
    dag_id="incremental_scd2_etl",
    default_args=default_args,
    description="ETL: incremental load + SCD2 update",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Manual trigger; za raspored stavite cron izraz (npr. "*/15 * * * *")
    catchup=False,
    tags=["incremental", "scd2", "etl"],
) as dag:

    # 1) Task: Pokreni Python skriptu za incremental load
    run_incremental_load = BashOperator(
        task_id="run_incremental_load",
        bash_command=(
            # Uvjerite se da je python u PATH; inače navedite punu putanju do python-a
            "python /opt/airflow/dags/incremental_load_python.py"
        ),
        cwd="/opt/airflow/dags",
        env={
            # Po potrebi dodajte varijable okruženja
        },
    )

    # 2) Task: Pokreni SCD2 SQL skriptu
    run_scd2_update = PostgresOperator(
        task_id="run_scd2_update",
        postgres_conn_id="postgres_analytical",
        sql="scd2_update.sql",
        autocommit=True,
    )

    # Definiraj redoslijed
    run_incremental_load >> run_scd2_update
