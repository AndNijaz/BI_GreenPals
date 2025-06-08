"""
DAG: star_schema_etl

Description:
    This Airflow DAG orchestrates the ETL process for creating and populating a star schema in the analytical database. It sequentially creates all dimension tables, performs SCD2 updates, and populates fact tables, supporting a robust data warehousing solution for analytical workloads.

Scheduling:
    [PLACEHOLDER: Specify scheduling requirements here. Currently, schedule_interval=None, so this DAG must be triggered manually.]

Author:
    ETL Team
    University Project – GreenPals / Company IoT
"""

# star_schema_etl.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "etl_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="star_schema_etl",
    default_args=default_args,
    description="ETL: Kreiranje i punjenje Star Schema (dimenzije + fact_readings)",
    start_date=datetime(2023, 1, 1),
    # schedule_interval="0 3 * * *",  # svaki dan u 03:00
    schedule_interval=None,  # svaki dan u 03:00
    catchup=False,
    tags=["star_schema", "silver_to_gold"],
) as dag:

    # -----------------------------------------------------------
    # 1) Kreiraj sve dimenzijske tablice (uključuje public.dim_time)
    # -----------------------------------------------------------
    run_create_dims = PostgresOperator(
        task_id="run_create_dims",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/01_create_dims.sql",
        autocommit=True,
    )

    # -----------------------------------------------------------
    # 2) Popuni dim_time putem SQL generate_series (umjesto Python loopa)
    # -----------------------------------------------------------
    run_populate_dim_time = PostgresOperator(
        task_id="run_populate_dim_time",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/02_populate_dim_time.sql",
        autocommit=True,
    )
    run_create_facts = PostgresOperator(
        task_id="run_create_facts",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/02_create_facts.sql",
        autocommit=True,
    )

    # -----------------------------------------------------------
    # 3) SCD2 update za dim_user
    # -----------------------------------------------------------
    run_scd2_dim_user = PostgresOperator(
        task_id="run_scd2_dim_user",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/03_scd2_dim_user.sql",
        autocommit=True,
    )

    # -----------------------------------------------------------
    # 4) SCD2 update za dim_location
    # -----------------------------------------------------------
    run_scd2_dim_location = PostgresOperator(
        task_id="run_scd2_dim_location",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/04_scd2_dim_location.sql",
        autocommit=True,
    )

    # -----------------------------------------------------------
    # 5) SCD2 update za dim_room
    # -----------------------------------------------------------
    run_scd2_dim_room = PostgresOperator(
        task_id="run_scd2_dim_room",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/05_scd2_dim_room.sql",
        autocommit=True,
    )

    # -----------------------------------------------------------
    # 6) SCD2 update za dim_device
    # -----------------------------------------------------------
    run_scd2_dim_device = PostgresOperator(
        task_id="run_scd2_dim_device",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/06_scd2_dim_device.sql",
        autocommit=True,
    )

    # -----------------------------------------------------------
    # 7) SCD2 update za dim_company
    # -----------------------------------------------------------
    run_scd2_dim_company = PostgresOperator(
        task_id="run_scd2_dim_company",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/07_scd2_dim_company.sql",
        autocommit=True,
    )

    # -----------------------------------------------------------
    # 8) SCD2 update za dim_department
    # -----------------------------------------------------------
    run_scd2_dim_department = PostgresOperator(
        task_id="run_scd2_dim_department",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/08_scd2_dim_department.sql",
        autocommit=True,
    )

    # -----------------------------------------------------------
    # 9) SCD2 update za dim_smart_plug
    # -----------------------------------------------------------
    run_scd2_dim_smart_plug = PostgresOperator(
        task_id="run_scd2_dim_smart_plug",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/09_scd2_dim_smart_plug.sql",
        autocommit=True,
    )

    # -----------------------------------------------------------
    # 10) Popuni fact_readings (Type 1 upsert)
    # -----------------------------------------------------------
    run_populate_fact = PostgresOperator(
        task_id="run_populate_fact_readings",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/10_populate_fact_readings.sql",
        autocommit=True,
    )

    run_create_fact_company_readings = PostgresOperator(
        task_id="run_create_fact_company_readings",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/03_create_fact_company_readings.sql",
        autocommit=True,
    )

    run_populate_fact_company_readings = PostgresOperator(
        task_id="run_populate_fact_company_readings",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/11_populate_fact_company_readings.sql",
        autocommit=True,
    )


     # -----------------------------------------------------------
    # 11) Kreiraj fact_plug_assignment i popuni
    # -----------------------------------------------------------
    run_create_fact_plug_assignment = PostgresOperator(
        task_id="run_create_fact_plug_assignment",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/12_create_fact_plug_assignment.sql",
        autocommit=True,
    )
    run_populate_fact_plug_assignment = PostgresOperator(
        task_id="run_populate_fact_plug_assignment",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/13_populate_fact_plug_assignment.sql",
        autocommit=True,
    )

    # -----------------------------------------------------------
    # 12) Kreiraj fact_device_events i popuni
    # -----------------------------------------------------------
    run_create_fact_device_events = PostgresOperator(
        task_id="run_create_fact_device_events",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/14_create_fact_device_events.sql",
        autocommit=True,
    )
    run_populate_fact_device_events = PostgresOperator(
        task_id="run_populate_fact_device_events",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/15_populate_fact_device_events.sql",
        autocommit=True,
    )

    # -----------------------------------------------------------
    # Definicija ovisnosti (DAG flow)
    # -----------------------------------------------------------
    (
        run_create_dims
        >> run_populate_dim_time
        >> run_create_facts
        >> run_scd2_dim_user
        >> run_scd2_dim_location
        >> run_scd2_dim_room
        >> run_scd2_dim_device
        >> run_scd2_dim_company
        >> run_scd2_dim_department
        >> run_scd2_dim_smart_plug
        >> run_populate_fact
        >> run_create_fact_company_readings 
        >> run_populate_fact_company_readings
        >> run_create_fact_plug_assignment
        >> run_populate_fact_plug_assignment
        >> run_create_fact_device_events
        >> run_populate_fact_device_events
    )
