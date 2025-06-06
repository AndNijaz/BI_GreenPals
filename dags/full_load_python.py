# full_load_python.py

import psycopg2
import tempfile
import os
import sys

# ------ Konfiguracija konekcija ------
OP_CONFIG = {
    "dbname": "db_operational",
    "user": "postgres",
    "password": "napoleonlm10",
    "host": "db_operational",   # Docker servis name, ne localhost
    "port": 5432
}

AN_CONFIG = {
    "dbname": "db_analytical",
    "user": "postgres",
    "password": "napoleonlm10",
    "host": "db_analytical",     # Docker servis name, ne localhost
    "port": 5432
}

# ------ Popis tablica za Full Load (redoslijed zbog FK) ------
TABLES_PUBLIC = [
    "users",
    "locations",
    "rooms",
    "devices",
    "smart_plugs",
    "plug_assignments",
    "readings"
]

TABLES_COMPANY = [
    "companies",
    "company_locations",
    "departments",
    "company_rooms",
    "company_devices",
    "company_users",
    "company_smart_plugs",
    "company_plug_assignments",
    "company_readings"
]

# ------ Karta landing imena tablica (public vs. company) ------
LANDING_PUBLIC = {
    "users":            "landing.users",
    "locations":        "landing.locations",
    "rooms":            "landing.rooms",
    "devices":          "landing.devices",
    "smart_plugs":      "landing.smart_plugs",
    "plug_assignments": "landing.plug_assignments",
    "readings":         "landing.readings"
}

LANDING_COMPANY = {
    "companies":                "landing.companies",
    "company_locations":        "landing.company_locations",
    "departments":              "landing.departments",
    "company_rooms":            "landing.company_rooms",
    "company_devices":          "landing.company_devices",
    "company_users":            "landing.company_users",
    "company_smart_plugs":      "landing.company_smart_plugs",
    "company_plug_assignments": "landing.company_plug_assignments",
    "company_readings":         "landing.company_readings"
}

# ------ Funkcija: Kreira landing sheme/tablce ako ne postoje ------
def ensure_landing_tables(an_conn):
    an_cur = an_conn.cursor()
    # 1) Kreiraj shemu landing ako ne postoji
    an_cur.execute("CREATE SCHEMA IF NOT EXISTS landing;")
    an_conn.commit()

    # 2) Public landing tablice
    an_cur.execute("""
        CREATE TABLE IF NOT EXISTS landing.users (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP NOT NULL,
            eco_points INTEGER NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL
        );
    """)
    an_cur.execute("""
        CREATE TABLE IF NOT EXISTS landing.locations (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            country TEXT NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)
    an_cur.execute("""
        CREATE TABLE IF NOT EXISTS landing.rooms (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            location_id INTEGER NOT NULL REFERENCES landing.locations(id),
            updated_at TIMESTAMP NOT NULL
        );
    """)
    an_cur.execute("""
        CREATE TABLE IF NOT EXISTS landing.devices (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)
    an_cur.execute("""
        CREATE TABLE IF NOT EXISTS landing.smart_plugs (
            id UUID PRIMARY KEY,
            user_id UUID NOT NULL,
            room_id INTEGER NOT NULL,
            device_id INTEGER NOT NULL,
            status BOOLEAN NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)
    an_cur.execute("""
        CREATE TABLE IF NOT EXISTS landing.plug_assignments (
            id INTEGER PRIMARY KEY,
            plug_id UUID NOT NULL,
            room_id INTEGER NOT NULL,
            device_id INTEGER NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)
    an_cur.execute("""
        CREATE TABLE IF NOT EXISTS landing.readings (
            id INTEGER PRIMARY KEY,
            plug_id UUID NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            power_kwh DECIMAL(6,3) NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)

    # 3) Company landing tablice
    an_cur.execute("""
        CREATE TABLE IF NOT EXISTS landing.companies (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            industry TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)
    an_cur.execute("""
        CREATE TABLE IF NOT EXISTS landing.company_locations (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            country TEXT NOT NULL,
            co2_factor DECIMAL NOT NULL,
            company_id UUID NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)
    an_cur.execute("""
        CREATE TABLE IF NOT EXISTS landing.departments (
            id INTEGER PRIMARY KEY,
            company_id UUID NOT NULL,
            name TEXT NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)
    an_cur.execute("""
        CREATE TABLE IF NOT EXISTS landing.company_rooms (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            location_id INTEGER NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)
    an_cur.execute("""
        CREATE TABLE IF NOT EXISTS landing.company_devices (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)
    an_cur.execute("""
        CREATE TABLE IF NOT EXISTS landing.company_users (
            id UUID PRIMARY KEY,
            company_id UUID NOT NULL,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            department_id INTEGER,
            role TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)
    an_cur.execute("""
        CREATE TABLE IF NOT EXISTS landing.company_smart_plugs (
            id UUID PRIMARY KEY,
            company_id UUID NOT NULL,
            room_id INTEGER NOT NULL,
            device_id INTEGER NOT NULL,
            status BOOLEAN NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)
    an_cur.execute("""
        CREATE TABLE IF NOT EXISTS landing.company_plug_assignments (
            id INTEGER PRIMARY KEY,
            plug_id UUID NOT NULL,
            room_id INTEGER NOT NULL,
            device_id INTEGER NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)
    an_cur.execute("""
        CREATE TABLE IF NOT EXISTS landing.company_readings (
            id INTEGER PRIMARY KEY,
            plug_id UUID NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            power_kwh DECIMAL(6,3) NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)
    an_conn.commit()
    an_cur.close()

# ------ Funkcija: TRUNCATE svih landing tablica (restart identity) ------
def truncate_landing(an_conn):
    an_cur = an_conn.cursor()
    all_landing = list(LANDING_PUBLIC.values()) + list(LANDING_COMPANY.values())
    # TRUNCATE i RESTART se podatke u landing briše, ali archive ostaje intaktan
    truncate_sql = "TRUNCATE TABLE " + ", ".join(all_landing) + " RESTART IDENTITY CASCADE;"
    print("\n→ Truncating landing tables...") 
    an_cur.execute(truncate_sql)
    an_conn.commit()
    an_cur.close()

# ------ Funkcija: Kopira toplelement (source) u dest_table via temp CSV fajl ------
def copy_table_via_tempfile(op_conn, an_conn, src_schema, src_table, dest_table):
    src_fq = f"{src_schema}.{src_table}"
    dest_fq = dest_table

    print(f"   • Copying {src_fq} → {dest_fq} ...", end="", flush=True)

    try:
        # 1) Kreiraj privremeni csv fajl (delete=False da kasnije sami obrišemo)
        with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as tmpfile:
            tmp_name = tmpfile.name

        # 2) Export iz operativne baze u tmpfile koristeći COPY ... TO STDOUT
        op_cur = op_conn.cursor()
        with open(tmp_name, "w", encoding="utf-8") as f:
            op_cur.copy_expert(f"COPY {src_fq} TO STDOUT WITH CSV HEADER", f)
        op_cur.close()

        # 3) Import iz tmpfile u analitičku landing tablicu koristeći COPY ... FROM STDIN
        an_cur = an_conn.cursor()
        with open(tmp_name, "r", encoding="utf-8") as f:
            an_cur.copy_expert(f"COPY {dest_fq} FROM STDIN WITH CSV HEADER", f)
        an_conn.commit()
        an_cur.close()

        # 4) Obriši privremeni csv fajl
        os.remove(tmp_name)

        print(" OK")
    except Exception as e:
        print(" ERROR:", e)
        an_conn.rollback()

def main():
    print("\n=== START: Full Load (Python + temp CSV) ===\n")

    # 1) Spoji se na db_operational
    try:
        op_conn = psycopg2.connect(**OP_CONFIG)
    except Exception as e:
        print("Greška: ne mogu se spojiti na db_operational:", e)
        sys.exit(1)

    # 2) Spoji se na db_analytical
    try:
        an_conn = psycopg2.connect(**AN_CONFIG)
    except Exception as e:
        print("Greška: ne mogu se spojiti na db_analytical:", e)
        op_conn.close()
        sys.exit(1)

    # 3) Kreiraj landing tablice ako ne postoje
    ensure_landing_tables(an_conn)

    # 4) Očisti landing (trunciraj sve redove)
    truncate_landing(an_conn)

    # 5) Kopiraj sve tablice iz public sheme
    print("\n→ Full Load: Public schema")
    for tbl in TABLES_PUBLIC:
        copy_table_via_tempfile(op_conn, an_conn, "public", tbl, LANDING_PUBLIC[tbl])

    # 6) Kopiraj sve tablice iz company_schema
    print("\n→ Full Load: Company schema")
    for tbl in TABLES_COMPANY:
        copy_table_via_tempfile(op_conn, an_conn, "company_schema", tbl, LANDING_COMPANY[tbl])

    # 7) Zatvori konekcije
    op_conn.close()
    an_conn.close()

    print("\n=== Full Load završen! ===")
    print("Možete sada pokrenuti SCD2 Update nad archive.* tablicama ili pustiti Airflow DAG.")
    sys.exit(0)

if __name__ == "__main__":
    main()
