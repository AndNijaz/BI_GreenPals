"""
DAG: full_scd2_etl

Description:
    This Airflow DAG performs a full ETL load from the operational database to the analytical landing layer, followed by SCD2 updates into the archive layer. It is designed for periodic or on-demand full refreshes of the data warehouse.

Scheduling:
    [PLACEHOLDER: Specify scheduling requirements here. Currently, schedule_interval=None, so this DAG must be triggered manually.]

Author:
    ETL Team
    University Project – GreenPals / Company IoT
"""

# full_scd2_etl.py

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
    dag_id="full_scd2_etl",
    default_args=default_args,
    description="ETL: full load + SCD2 update",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Manual trigger; može se promijeniti u npr. "@daily"
    catchup=False,
    tags=["full_load", "scd2", "etl"],
) as dag:

    # 1) Task: Pokreni Python skriptu za Full Load
    run_full_load = BashOperator(
        task_id="run_full_load",
        bash_command=(
            # Uvjerite se da je Python dostupan unutar PATH-a u Airflow image
            "python full_load_python.py"
        ),
        cwd="/opt/airflow/dags",
        env={},
    )

    # 2) Task: Pokreni SCD2 SQL skriptu (istraživanje i upis u archive.*)
    run_scd2_update = PostgresOperator(
        task_id="run_scd2_update",
        postgres_conn_id="postgres_analytical",  # mora biti već definirana konekcija
        sql="scd2_update.sql",                   # samo naziv, Airflow ga gleda u /opt/airflow/dags/
        autocommit=True,
    )

    # Definiraj redoslijed: prvo full_load → zatim SCD2 update
    run_full_load >> run_scd2_update
