from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    "owner": "etl_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="api_to_landing",
    default_args=default_args,
    start_date=datetime(2023,1,1),
    schedule_interval="0 * * * *",  # svaki sat
    catchup=False,
    tags=["landing","api"]
) as dag:

    # --- CO₂ factors ---
    fetch_co2 = SimpleHttpOperator(
        task_id="fetch_co2",
        http_conn_id="co2-api-http",     # vidi korak 5
        endpoint="co2-factors",
        method="GET",
        response_filter=lambda r: r.json(),
        do_xcom_push=True,
    )

    def load_co2(ti):
        rows = ti.xcom_pull(task_ids="fetch_co2")
        pg = PostgresHook(postgres_conn_id="postgres_analytical")
        # truncate + batch insert
        pg.run("TRUNCATE landing.co2_factors;")
        pg.insert_rows(
            table="landing.co2_factors",
            rows=[(r["source_name"],r["country"],r["co2_factor"],r["unit"],r["updated_at"]) for r in rows],
            target_fields=None,
            commit_every=1000
        )

    load_co2 = PythonOperator(
        task_id="load_co2",
        python_callable=load_co2,
    )


    # --- electricity prices ---
    fetch_prices = SimpleHttpOperator(
        task_id="fetch_prices",
        http_conn_id="price-api-http",   # vidi korak 5
        endpoint="electricity-prices",
        method="GET",
        response_filter=lambda r: r.json(),
        do_xcom_push=True,
    )

    def load_prices(ti):
        rows = ti.xcom_pull(task_ids="fetch_prices")
        pg = PostgresHook(postgres_conn_id="postgres_analytical")
        pg.run("TRUNCATE landing.electricity_prices;")
        pg.insert_rows(
            table="landing.electricity_prices",
            rows=[(r["country"], r["price_per_kwh"], ) for r in rows],
            target_fields=["country","price_per_kwh"],
            commit_every=1000
        )

    load_prices = PythonOperator(
        task_id="load_prices",
        python_callable=load_prices,
    )

    fetch_co2 >> load_co2 >> fetch_prices >> load_prices
