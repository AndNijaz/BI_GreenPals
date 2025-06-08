# -------------------------------------------------------------
#  incremental_load_python.py
#  → operaciona baza + API  → landing.*
# -------------------------------------------------------------
import psycopg2, requests, sys
from psycopg2.extras import execute_values
from datetime import datetime
from tables_config import TABLES_PUBLIC, TABLES_COMPANY

# ---------- 1) DB konekcije ----------
OP_CONFIG = {
    "dbname": "db_operational",
    "user": "postgres",
    "password": "napoleonlm10",
    "host": "db_operational",
    "port": 5432
}
AN_CONFIG = {
    "dbname": "db_analytical",
    "user": "postgres",
    "password": "napoleonlm10",
    "host": "db_analytical",
    "port": 5432
}

# ---------- 2) Operacione meta-tabele ----------
ALL_TABLES = {**TABLES_PUBLIC, **TABLES_COMPANY}

# ---------- 3) API meta-tabele ----------
API_TABLES = {
    "co2_factors": {
        "endpoint": "http://host.docker.internal:8000/co2-factors",
        "landing":  "landing.co2_factors",
        "columns":  ["source_name", "country", "co2_factor", "unit", "updated_at"],
        "pk":       ["source_name", "country"]
    },
    "electricity_prices": {
        "endpoint": "http://host.docker.internal:8001/electricity-prices",
        "landing":  "landing.electricity_prices",
        "columns":  ["country", "price_per_kwh", "updated_at"],
        "pk":       ["country"]
    }
}

# ---------- 4) Helper funkcije ----------
def upsert_rows(conn, landing, cols, pk_cols, rows):
    if not rows:
        return
    col_list = ", ".join(cols)
    pk_list  = ", ".join(pk_cols)
    upd_list = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c not in pk_cols)
    sql = f"""
        INSERT INTO {landing} ({col_list})
        VALUES %s
        ON CONFLICT ({pk_list}) DO UPDATE SET {upd_list};
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()

def max_updated(conn, landing):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COALESCE(MAX(updated_at),'2000-01-01') FROM {landing};")
        return cur.fetchone()[0]

def fetch_api_rows(endpoint, since_iso):
    r = requests.get(endpoint, params={"since": since_iso}, timeout=15)
    r.raise_for_status()
    return r.json()   # list[dict]

# ---------- 5) Glavni proces ----------
def incremental_load():
    op = psycopg2.connect(**OP_CONFIG)
    an = psycopg2.connect(**AN_CONFIG)

    # 5.1  Operaciona baza → landing.*
    for tbl, meta in ALL_TABLES.items():
        last = max_updated(an, meta["landing"])
        select_cols = ", ".join(meta["columns"])
        src_table   = f"{meta['schema']}.{tbl}"

        with op.cursor() as cur:
            cur.execute(
                f"SELECT {select_cols} FROM {src_table} WHERE updated_at > %s;",
                (last,)
            )
            rows = cur.fetchall()

        print(f"[OP] {src_table} → {meta['landing']}: {len(rows)} rows")
        upsert_rows(an, meta["landing"], meta["columns"], ["id"], rows)

    # 5.2  API → landing.*
    for tbl, meta in API_TABLES.items():
        last = max_updated(an, meta["landing"])
        rows_json = fetch_api_rows(meta["endpoint"], last.isoformat())
        rows = [
            tuple(j.get(c, None) for c in meta["columns"][:-1]) + (datetime.utcnow(),)
            for j in rows_json
        ]

        print(f"[API] {tbl} → {meta['landing']}: {len(rows)} rows")
        upsert_rows(an, meta["landing"], meta["columns"], meta["pk"], rows)

    op.close()
    an.close()
    print("=== Incremental → landing završeno ===")

if __name__ == "__main__":
    incremental_load()
