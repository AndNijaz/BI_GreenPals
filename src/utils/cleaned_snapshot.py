# -------------------------------------------------
#   cleaned_snapshot.py   (FINAL VERSION)
#   • Puni cleaned.* snapshot iz archive.*
#   • Sve ACTIVE (end_date = '9999-12-31') redove
#   • Ako tablica ima "dedup_pk": zadrži najnoviji
# -------------------------------------------------
import psycopg2
from datetime import datetime

AN_CONFIG = {
    "dbname": "db_analytical",
    "user": "postgres",
    "password": "napoleonlm10",
    "host": "db_analytical",
    "port": 5432
}

TABLE_MAP = {
    # ========== PUBLIC ==========
    "users": {
        "cols":  "user_id, name, email, eco_points, updated_at",
        "exprs": "user_id, name, email, eco_points, updated AS updated_at"
    },
    "devices": {
        "cols":  "device_id, name, category, updated_at",
        "exprs": "device_id, name, category, updated AS updated_at"
    },
    "smart_plugs": {
        "cols":  "plug_id, user_id, room_id, device_id, status, updated_at",
        "exprs": "plug_id, user_id, room_id, device_id, status, updated AS updated_at"
    },
    "plug_assignments": {
        "cols":  "assignment_id, plug_id, room_id, device_id, start_time, end_time, updated_at",
        "exprs": "assignment_id, plug_id, room_id, device_id, start_time, end_time, updated AS updated_at"
    },
    "readings": {
        "cols":  "plug_id, timestamp, power_kwh, updated_at",
        "exprs": "plug_id, timestamp, power_kwh, updated AS updated_at",
        "dedup_pk": "plug_id, timestamp"
    },
    "locations": {
        "cols":  "location_id, name, country, updated_at",
        "exprs": "location_id, name, country, updated AS updated_at"
    },
    "rooms": {
        "cols":  "room_id, name, location_id, updated_at",
        "exprs": "room_id, name, location_id, updated AS updated_at"
    },

    # ========== COMPANY ==========
    "companies": {
        "cols":  "company_id, name, industry, updated_at",
        "exprs": "company_id, name, industry, updated AS updated_at"
    },
    "company_locations": {
        "cols":  "company_location_id, name, country, co2_factor, company_id, updated_at",
        "exprs": "company_location_id, name, country, co2_factor, company_id, updated AS updated_at"
    },
    "departments": {
        "cols":  "department_id, company_id, name, updated_at",
        "exprs": "department_id, company_id, name, updated AS updated_at"
    },
    "company_rooms": {
        "cols":  "company_room_id, name, location_id, updated_at",
        "exprs": "company_room_id, name, location_id, updated AS updated_at"
    },
    "company_devices": {
        "cols":  "device_id, name, category, updated_at",
        "exprs": "device_id, name, category, updated AS updated_at"
    },
    "company_users": {
        "cols":  "user_id, company_id, name, email, department_id, role, updated_at",
        "exprs": "user_id, company_id, name, email, department_id, role, updated AS updated_at"
    },
    "company_smart_plugs": {
        "cols":  "plug_id, company_id, room_id, device_id, status, updated_at",
        "exprs": "plug_id, company_id, room_id, device_id, status, updated AS updated_at"
    },
    "company_plug_assignments": {
        "cols":  "company_plug_assignment_id, plug_id, room_id, device_id, start_time, end_time, updated_at",
        "exprs": "company_plug_assignment_id, plug_id, room_id, device_id, start_time, end_time, updated AS updated_at"
    },
    "company_readings": {
        "cols":  "plug_id, timestamp, power_kwh, updated_at",
        "exprs": "plug_id, timestamp, power_kwh, updated AS updated_at",
        "dedup_pk": "plug_id, timestamp"
    },

    # ========== EXTERNAL ==========
    "co2_factors": {
    "cols":  "source_name, country, co2_factor, unit, updated_at",
    "exprs": "source_name, country, co2_factor, unit, updated AS updated_at"
    },
    "electricity_prices": {
        "cols":  "country, price_per_kwh, updated_at",
        "exprs": "country, price_per_kwh, updated AS updated_at"
    }
}


def refresh_cleaned_snapshot():
    conn = psycopg2.connect(**AN_CONFIG)
    cur  = conn.cursor()

    for tbl, meta in TABLE_MAP.items():
        cur.execute(f"TRUNCATE cleaned.{tbl};")

        if "dedup_pk" in meta:
            # Zadrži najnoviju (po updated_at) verziju za svaki prirodni ključ
            dedup_sql = f"""
                INSERT INTO cleaned.{tbl} ({meta['cols']})
                SELECT DISTINCT ON ({meta['dedup_pk']})
                       {meta['exprs']}
                  FROM archive.{tbl}
                 WHERE end_date = '9999-12-31'
              ORDER BY {meta['dedup_pk']}, updated DESC;
            """
            cur.execute(dedup_sql)
        else:
            cur.execute(f"""
                INSERT INTO cleaned.{tbl} ({meta['cols']})
                SELECT {meta['exprs']}
                  FROM archive.{tbl}
                 WHERE end_date = '9999-12-31';
            """)
        print(f"✓ cleaned.{tbl} refreshed")

    conn.commit()
    conn.close()


# Run standalone for quick test (optional)
if __name__ == "__main__":
    print(f"Starting snapshot refresh @ {datetime.utcnow().isoformat()}")
    refresh_cleaned_snapshot()
    print("Done!")
