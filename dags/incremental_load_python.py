# incremental_load_python.py

import psycopg2
from psycopg2.extras import execute_values
import sys

# ================================================
# 1) Konfiguracija konekcija
# ================================================
OP_CONFIG = {
    "dbname": "db_operational",
    "user": "postgres",
    "password": "napoleonlm10",
    "host": "db_operational",   # vidjeti napomenu niže
    "port": 5432
}

AN_CONFIG = {
    "dbname": "db_analytical",
    "user": "postgres",
    "password": "napoleonlm10",
    "host": "db_analytical",     # vidjeti napomenu niže
    "port": 5432
}

# ================================================
# 2) Definicija tablica i njihovih kolona
# ================================================
TABLES_PUBLIC = {
    "users": {
        "schema": "public",
        "landing": "landing.users",
        "columns": [
            "id", "name", "email", "eco_points", "created_at", "updated_at"
        ]
    },
    "locations": {
        "schema": "public",
        "landing": "landing.locations",
        "columns": [
            "id", "name", "country", "updated_at"
        ]
    },
    "rooms": {
        "schema": "public",
        "landing": "landing.rooms",
        "columns": [
            "id", "name", "location_id", "updated_at"
        ]
    },
    "devices": {
        "schema": "public",
        "landing": "landing.devices",
        "columns": [
            "id", "name", "category", "created_at", "updated_at"
        ]
    },
    "smart_plugs": {
        "schema": "public",
        "landing": "landing.smart_plugs",
        "columns": [
            "id", "user_id", "room_id", "device_id", "status", "created_at", "updated_at"
        ]
    },
    "plug_assignments": {
        "schema": "public",
        "landing": "landing.plug_assignments",
        "columns": [
            "id", "plug_id", "room_id", "device_id", "start_time", "end_time", "created_at", "updated_at"
        ]
    },
    "readings": {
        "schema": "public",
        "landing": "landing.readings",
        "columns": [
            "id", "plug_id", "timestamp", "power_kwh", "created_at", "updated_at"
        ]
    }
}

TABLES_COMPANY = {
    "companies": {
        "schema": "company_schema",
        "landing": "landing.companies",
        "columns": [
            "id", "name", "industry", "created_at", "updated_at"
        ]
    },
    "company_locations": {
        "schema": "company_schema",
        "landing": "landing.company_locations",
        "columns": [
            "id", "name", "country", "co2_factor", "company_id", "updated_at"
        ]
    },
    "departments": {
        "schema": "company_schema",
        "landing": "landing.departments",
        "columns": [
            "id", "company_id", "name", "updated_at"
        ]
    },
    "company_rooms": {
        "schema": "company_schema",
        "landing": "landing.company_rooms",
        "columns": [
            "id", "name", "location_id", "updated_at"
        ]
    },
    "company_devices": {
        "schema": "company_schema",
        "landing": "landing.company_devices",
        "columns": [
            "id", "name", "category", "created_at", "updated_at"
        ]
    },
    "company_users": {
        "schema": "company_schema",
        "landing": "landing.company_users",
        "columns": [
            "id", "company_id", "name", "email", "department_id", "role", "created_at", "updated_at"
        ]
    },
    "company_smart_plugs": {
        "schema": "company_schema",
        "landing": "landing.company_smart_plugs",
        "columns": [
            "id", "company_id", "room_id", "device_id", "status", "created_at", "updated_at"
        ]
    },
    "company_plug_assignments": {
        "schema": "company_schema",
        "landing": "landing.company_plug_assignments",
        "columns": [
            "id", "plug_id", "room_id", "device_id", "start_time", "end_time", "created_at", "updated_at"
        ]
    },
    "company_readings": {
        "schema": "company_schema",
        "landing": "landing.company_readings",
        "columns": [
            "id", "plug_id", "timestamp", "power_kwh", "created_at", "updated_at"
        ]
    }
}

ALL_TABLES = {}
ALL_TABLES.update(TABLES_PUBLIC)
ALL_TABLES.update(TABLES_COMPANY)


def upsert_rows(an_conn, landing_table, columns, rows):
    if not rows:
        return

    col_list = ", ".join(columns)
    pk = "id"
    update_list = [f"{c} = EXCLUDED.{c}" for c in columns if c != pk]
    update_clause = ", ".join(update_list)

    sql = f"""
        INSERT INTO {landing_table} ({col_list})
        VALUES %s
        ON CONFLICT ({pk})
        DO UPDATE 
          SET {update_clause};
    """

    with an_conn.cursor() as cur:
        from psycopg2.extras import execute_values
        execute_values(cur, sql, rows)
    an_conn.commit()


def incremental_load(op_conn, an_conn):
    for tbl_name, info in ALL_TABLES.items():
        schema = info["schema"]
        landing = info["landing"]
        cols = info["columns"]

        with an_conn.cursor() as cur_an:
            cur_an.execute(f"SELECT COALESCE(MAX(updated_at), '2000-01-01') FROM {landing};")
            last_updated = cur_an.fetchone()[0]

        select_cols = ", ".join(cols)
        op_query = (
            f"SELECT {select_cols} "
            f"FROM {schema}.{tbl_name} "
            f"WHERE updated_at > %s"
        )
        with op_conn.cursor() as cur_op:
            cur_op.execute(op_query, (last_updated,))
            rows = cur_op.fetchall()

        print(f"→ `{schema}.{tbl_name}`: pronađeno {len(rows):,} novih/izmijenjenih redova (since {last_updated}).")
        if rows:
            upsert_rows(an_conn, landing, cols, rows)


def main():
    print("\n=== Pokretanje Incremental Load (Python) ===\n")

    try:
        op_conn = psycopg2.connect(**OP_CONFIG)
    except Exception as e:
        print("Greška: ne mogu se spojiti na db_operational:", e)
        sys.exit(1)

    try:
        an_conn = psycopg2.connect(**AN_CONFIG)
    except Exception as e:
        print("Greška: ne mogu se spojiti na db_analytical:", e)
        op_conn.close()
        sys.exit(1)

    incremental_load(op_conn, an_conn)

    op_conn.close()
    an_conn.close()

    print("\n=== Incremental Load završen! ===")
    print("Možete sada pokrenuti SCD2 UPDATE nad archive.* tablicama.\n")


if __name__ == "__main__":
    main()
