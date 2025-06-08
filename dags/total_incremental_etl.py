# --------------------------------------------------------------
#  total_incremental_etl.py
#  → kompletan incremental pipeline (*/15 min ili ručno)
# --------------------------------------------------------------
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from utils.cleaned_snapshot import refresh_cleaned_snapshot
from airflow.operators.email import EmailOperator




default_args = {
    "owner": "etl_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="total_incremental_etl",
    description="Incremental: landing → archive → cleaned → star-schema",
    start_date=datetime(2023, 1, 1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["incremental", "star_schema"]
) as dag:

    t1 = BashOperator(
        task_id="incremental_to_landing",
        bash_command="python /opt/airflow/dags/incremental_load_python.py",
        cwd="/opt/airflow/dags"
    )

    t2 = PostgresOperator(
        task_id="scd2_update_archive",
        postgres_conn_id="postgres_analytical",
        sql="scd2_update.sql",
        autocommit=True
    )

    t3 = PythonOperator(
        task_id="refresh_cleaned_snapshot",
        python_callable=refresh_cleaned_snapshot
    )

    t4 = PostgresOperator(
        task_id="scd2_update_archive_cleaned",
        postgres_conn_id="postgres_analytical",
        sql="scd2_cleaned.sql",
        autocommit=True
    )

    t5 = PostgresOperator(
        task_id="incremental_star_schema",
        postgres_conn_id="postgres_analytical",
        sql="star-schema/16_incremental_star_schema.sql",
        autocommit=True
    )

    send_success_email = EmailOperator(
        task_id="send_success_email",
        to="admin@greenpals.io",
        subject="✅ ETL Pipeline Finished Successfully",
        html_content="""
        <h3>GreenPals ETL Status</h3>
        <p>The ETL pipeline for the star schema has been completed successfully at {{ execution_date }}.</p>
        <p>All layers (archive, cleaned, and star) are now up to date.</p>
        """,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> send_success_email
