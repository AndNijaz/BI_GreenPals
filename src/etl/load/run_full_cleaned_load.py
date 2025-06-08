#!/usr/bin/env python3
import os
import psycopg2

# ─── 1) Konfiguracija konekcije ────────────────────────────────────────────────
DB_PARAMS = {
    "host": os.getenv("ANALYTICAL_DB_HOST", "db_analytical"),
    "port": int(os.getenv("ANALYTICAL_DB_PORT", 5432)),
    "dbname": os.getenv("ANALYTICAL_DB_NAME", "db_analytical"),
    "user": os.getenv("ANALYTICAL_DB_USER", "postgres"),
    "password": os.getenv("ANALYTICAL_DB_PASSWORD", "napoleonlm10"),
}

# ─── 2) Popis svih cleaned tablica koje treba truncate-ati ────────────────────
CLEANED_TABLES = [
    "cleaned.users",
    "cleaned.locations",
    "cleaned.rooms",
    "cleaned.devices",
    "cleaned.smart_plugs",
    "cleaned.plug_assignments",
    "cleaned.readings",
    "cleaned.companies",
    "cleaned.company_locations",
    "cleaned.departments",
    "cleaned.company_rooms",
    "cleaned.company_devices",
    "cleaned.company_users",
    "cleaned.company_smart_plugs",
    "cleaned.company_plug_assignments",
    "cleaned.company_readings",
    "cleaned.co2_factors",
    "cleaned.electricity_prices",
]

# ─── 3) Redoslijed SQL fajlova ─────────────────────────────────────────────────
SQL_FILES = [
    ("full_load_cleaned.sql", "Full load into cleaned.*"),
    ("scd2_cleaned.sql",      "Populate archive_cleaned.* via SCD2"),
]

# ─── 4) Truncate funkcija ───────────────────────────────────────────────────────
def truncate_cleaned(conn):
    sql = "TRUNCATE TABLE " + ", ".join(CLEANED_TABLES) + " RESTART IDENTITY CASCADE;"
    print("\n→ Truncating cleaned tables…")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print("✅ cleaned tables truncated")

# ─── 5) Execute SQL fajl ────────────────────────────────────────────────────────
def run_sql(conn, filename, description):
    here = os.path.dirname(__file__)
    path = os.path.join(here, filename)
    print(f"\n→ Executing `{filename}` ({description})…")
    with open(path, "r") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(f"✅ `{filename}` done")

# ─── 6) Glavna rutina ─────────────────────────────────────────────────────────
def main():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
    except Exception as e:
        print("❌ Ne mogu se spojiti na db_analytical:", e)
        return

    # 1) Očisti cleaned shemu
    truncate_cleaned(conn)

    # 2) Učitaj full load u cleaned.* tablice
    for filename, desc in SQL_FILES:
        run_sql(conn, filename, desc)

    conn.close()
    print("\n🎉 Sve SQL skripte su uspješno izvršene!")

if __name__ == "__main__":
    main()
