# populate_cleaned_python.py

import psycopg2
import sys

# ================================================
# 1) Konfiguracija konekcija (samo analytical baza)
# ================================================
AN_CONFIG = {
    "dbname": "db_analytical",
    "user": "postgres",
    "password": "napoleonlm10",
    "host": "db_analytical",  # Docker servis name
    "port": 5432
}

# ================================================
# 2) Mapa entiteta → archive_raw i cleaned sheme
# ================================================
ARCHIVE_RAW_TABLES = {
    "users": {
        "archive_tbl": "archive.users",
        "cleaned_tbl": "cleaned.users",
        # u archive.users su kolone: user_id, name, email, eco_points, updated
        "columns": ["user_id", "name", "email", "eco_points", "updated"]
    },
    "locations": {
        "archive_tbl": "archive.locations",
        "cleaned_tbl": "cleaned.locations",
        "columns": ["location_id", "name", "country", "updated"]
    },
    "rooms": {
        "archive_tbl": "archive.rooms",
        "cleaned_tbl": "cleaned.rooms",
        "columns": ["room_id", "name", "location_id", "updated"]
    },
    "devices": {
        "archive_tbl": "archive.devices",
        "cleaned_tbl": "cleaned.devices",
        "columns": ["device_id", "name", "category", "updated"]
    },
    "smart_plugs": {
        "archive_tbl": "archive.smart_plugs",
        "cleaned_tbl": "cleaned.smart_plugs",
        "columns": ["plug_id", "user_id", "room_id", "device_id", "status", "updated"]
    },
    "plug_assignments": {
        "archive_tbl": "archive.plug_assignments",
        "cleaned_tbl": "cleaned.plug_assignments",
        "columns": ["assignment_id", "plug_id", "room_id", "device_id", "start_time", "end_time", "updated"]
    },
    "readings": {
        "archive_tbl": "archive.readings",
        "cleaned_tbl": "cleaned.readings",
        "columns": ["plug_id", "timestamp", "power_kwh", "updated"]
    },
    # Company entiteti:
    "companies": {
        "archive_tbl": "archive.companies",
        "cleaned_tbl": "cleaned.companies",
        "columns": ["company_id", "name", "industry", "updated"]
    },
    "company_locations": {
        "archive_tbl": "archive.company_locations",
        "cleaned_tbl": "cleaned.company_locations",
        "columns": ["company_location_id", "name", "country", "co2_factor", "company_id", "updated"]
    },
    "departments": {
        "archive_tbl": "archive.departments",
        "cleaned_tbl": "cleaned.departments",
        "columns": ["department_id", "company_id", "name", "updated"]
    },
    "company_rooms": {
        "archive_tbl": "archive.company_rooms",
        "cleaned_tbl": "cleaned.company_rooms",
        "columns": ["company_room_id", "name", "location_id", "updated"]
    },
    "company_devices": {
        "archive_tbl": "archive.company_devices",
        "cleaned_tbl": "cleaned.company_devices",
        "columns": ["device_id", "name", "category", "updated"]
    },
    "company_users": {
        "archive_tbl": "archive.company_users",
        "cleaned_tbl": "cleaned.company_users",
        "columns": ["user_id", "company_id", "name", "email", "department_id", "role", "updated"]
    },
    "company_smart_plugs": {
        "archive_tbl": "archive.company_smart_plugs",
        "cleaned_tbl": "cleaned.company_smart_plugs",
        "columns": ["plug_id", "company_id", "room_id", "device_id", "status", "updated"]
    },
    "company_plug_assignments": {
        "archive_tbl": "archive.company_plug_assignments",
        "cleaned_tbl": "cleaned.company_plug_assignments",
        "columns": ["company_plug_assignment_id", "plug_id", "room_id", "device_id", "start_time", "end_time", "updated"]
    },
    "company_readings": {
        "archive_tbl": "archive.company_readings",
        "cleaned_tbl": "cleaned.company_readings",
        "columns": ["plug_id", "timestamp", "power_kwh", "updated"]
    }
}


def clean_value(val):
    """
    Jednostavno čišćenje: ako je None, vrati None;
    ako je string, trim i return; inače return onako kako jest.
    """
    if val is None:
        return None
    if isinstance(val, str):
        return val.strip()
    return val


def truncate_cleaned_table(cur, cleaned_tbl):
    """
    TRUNCATE na cleaned shemu prije punjenja.
    """
    cur.execute(f"TRUNCATE TABLE {cleaned_tbl} RESTART IDENTITY CASCADE;")


def populate_cleaned_table(cur, info):
    """
    Popunjava jednu cleaned tablicu iz odgovarajućeg archive_raw entiteta,
    pri čemu za readings (i company_readings) koristimo DISTINCT ON kako bismo
    izbacili duplikate po (plug_id, timestamp).
    """
    archive_tbl = info["archive_tbl"]
    cleaned_tbl = info["cleaned_tbl"]
    cols = info["columns"]
    col_list = ", ".join(cols)

    # 1) SELECT aktivnih redova iz archive_raw (end_date = '9999-12-31')
    #    Za readings (i company_readings) koristimo DISTINCT ON da bismo izbacili duplikate.
    if archive_tbl.endswith(".readings"):
        # Koristimo DISTINCT ON (plug_id, timestamp), uz ORDER BY updated DESC
        # kako bismo zadržali samo onaj red s najnovijim 'updated'
        sql = f"""
            SELECT DISTINCT ON (plug_id, "timestamp")
                   {col_list}
              FROM {archive_tbl}
             WHERE end_date = '9999-12-31'
             ORDER BY plug_id, "timestamp", updated DESC;
        """
    else:
        sql = f"SELECT {col_list} FROM {archive_tbl} WHERE end_date = '9999-12-31';"

    cur.execute(sql)
    rows = cur.fetchall()

    if not rows:
        print(f"→ Nema aktivnih redova u {archive_tbl}, preskačem {cleaned_tbl}.")
        return

    # 2) Pripremi listu "čišćenih" redova za INSERT
    cleaned_rows = []
    for row in rows:
        # Ovisno o entitetu, raspakiramo tuple i mapiramo na cleaned shemu.
        # Ako cleaned shema ima polje created_at, ubacujemo None (ili updated).
        if cleaned_tbl == "cleaned.users":
            # row: (user_id, name, email, eco_points, updated)
            user_id, name, email, eco_points, updated = row
            cleaned_rows.append((
                clean_value(user_id),
                clean_value(name),
                clean_value(email).lower(),
                clean_value(eco_points) or 0,
                None,               # created_at
                clean_value(updated)  # updated_at
            ))

        elif cleaned_tbl == "cleaned.locations":
            # row: (location_id, name, country, updated)
            location_id, name, country, updated = row
            cleaned_rows.append((
                clean_value(location_id),
                clean_value(name),
                clean_value(country),
                clean_value(updated)   # updated_at
            ))

        elif cleaned_tbl == "cleaned.rooms":
            # row: (room_id, name, location_id, updated)
            room_id, name, location_id, updated = row
            cleaned_rows.append((
                clean_value(room_id),
                clean_value(name),
                clean_value(location_id),
                clean_value(updated)   # updated_at
            ))

        elif cleaned_tbl == "cleaned.devices":
            # row: (device_id, name, category, updated)
            device_id, name, category, updated = row
            cleaned_rows.append((
                clean_value(device_id),
                clean_value(name),
                clean_value(category),
                None,                  # created_at
                clean_value(updated)   # updated_at
            ))

        elif cleaned_tbl == "cleaned.smart_plugs":
            # row: (plug_id, user_id, room_id, device_id, status, updated)
            plug_id, user_id, room_id, device_id, status, updated = row
            cleaned_rows.append((
                clean_value(plug_id),
                clean_value(user_id),
                clean_value(room_id),
                clean_value(device_id),
                clean_value(status),
                None,                  # created_at
                clean_value(updated)   # updated_at
            ))

        elif cleaned_tbl == "cleaned.plug_assignments":
            # row: (assignment_id, plug_id, room_id, device_id, start_time, end_time, updated)
            assignment_id, plug_id, room_id, device_id, start_time, end_time, updated = row
            cleaned_rows.append((
                clean_value(assignment_id),
                clean_value(plug_id),
                clean_value(room_id),
                clean_value(device_id),
                clean_value(start_time),
                clean_value(end_time),
                None,                  # created_at
                clean_value(updated)   # updated_at
            ))

        elif cleaned_tbl == "cleaned.readings":
            # row: (plug_id, timestamp, power_kwh, updated)
            plug_id, timestamp, power_kwh, updated = row
            cleaned_rows.append((
                clean_value(plug_id),
                clean_value(timestamp),
                clean_value(power_kwh),
                None,                  # created_at
                clean_value(updated)   # updated_at
            ))

        elif cleaned_tbl == "cleaned.companies":
            # row: (company_id, name, industry, updated)
            company_id, name, industry, updated = row
            cleaned_rows.append((
                clean_value(company_id),
                clean_value(name),
                clean_value(industry),
                None,                  # created_at
                clean_value(updated)   # updated_at
            ))

        elif cleaned_tbl == "cleaned.company_locations":
            # row: (company_location_id, name, country, co2_factor, company_id, updated)
            cloc_id, name, country, co2_factor, company_id, updated = row
            cleaned_rows.append((
                clean_value(cloc_id),
                clean_value(name),
                clean_value(country),
                clean_value(co2_factor),
                clean_value(company_id),
                clean_value(updated)   # updated_at
            ))

        elif cleaned_tbl == "cleaned.departments":
            # row: (department_id, company_id, name, updated)
            dept_id, company_id, name, updated = row
            cleaned_rows.append((
                clean_value(dept_id),
                clean_value(company_id),
                clean_value(name),
                clean_value(updated)   # updated_at
            ))

        elif cleaned_tbl == "cleaned.company_rooms":
            # row: (company_room_id, name, location_id, updated)
            crow_id, name, location_id, updated = row
            cleaned_rows.append((
                clean_value(crow_id),
                clean_value(name),
                clean_value(location_id),
                clean_value(updated)   # updated_at
            ))

        elif cleaned_tbl == "cleaned.company_devices":
            # row: (device_id, name, category, updated)
            device_id, name, category, updated = row
            cleaned_rows.append((
                clean_value(device_id),
                clean_value(name),
                clean_value(category),
                None,                  # created_at
                clean_value(updated)   # updated_at
            ))

        elif cleaned_tbl == "cleaned.company_users":
            # row: (user_id, company_id, name, email, department_id, role, updated)
            user_id, company_id, name, email, department_id, role, updated = row
            cleaned_rows.append((
                clean_value(user_id),
                clean_value(company_id),
                clean_value(name),
                clean_value(email).lower(),
                clean_value(department_id),
                clean_value(role),
                None,                  # created_at
                clean_value(updated)   # updated_at
            ))

        elif cleaned_tbl == "cleaned.company_smart_plugs":
            # row: (plug_id, company_id, room_id, device_id, status, updated)
            plug_id, company_id, room_id, device_id, status, updated = row
            cleaned_rows.append((
                clean_value(plug_id),
                clean_value(company_id),
                clean_value(room_id),
                clean_value(device_id),
                clean_value(status),
                None,                  # created_at
                clean_value(updated)   # updated_at
            ))

        elif cleaned_tbl == "cleaned.company_plug_assignments":
            # row: (company_plug_assignment_id, plug_id, room_id, device_id, start_time, end_time, updated)
            cpa_id, plug_id, room_id, device_id, start_time, end_time, updated = row
            cleaned_rows.append((
                clean_value(cpa_id),
                clean_value(plug_id),
                clean_value(room_id),
                clean_value(device_id),
                clean_value(start_time),
                clean_value(end_time),
                None,                  # created_at
                clean_value(updated)   # updated_at
            ))

        elif cleaned_tbl == "cleaned.company_readings":
            # row: (plug_id, timestamp, power_kwh, updated)
            plug_id, timestamp, power_kwh, updated = row
            cleaned_rows.append((
                clean_value(plug_id),
                clean_value(timestamp),
                clean_value(power_kwh),
                None,                  # created_at
                clean_value(updated)   # updated_at
            ))

        else:
            raise RuntimeError(f"Nepoznat cleaned_tbl: {cleaned_tbl}")

    # 3) Truncate i batch INSERT
    truncate_cleaned_table(cur, cleaned_tbl)

    # Pripremi INSERT‐skriptu (ovisno o cleaned_tbl)
    if cleaned_tbl == "cleaned.users":
        insert_sql = """
            INSERT INTO cleaned.users
              (user_id, name, email, eco_points, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
    elif cleaned_tbl == "cleaned.locations":
        insert_sql = """
            INSERT INTO cleaned.locations
              (location_id, name, country, updated_at)
            VALUES (%s, %s, %s, %s);
        """
    elif cleaned_tbl == "cleaned.rooms":
        insert_sql = """
            INSERT INTO cleaned.rooms
              (room_id, name, location_id, updated_at)
            VALUES (%s, %s, %s, %s);
        """
    elif cleaned_tbl == "cleaned.devices":
        insert_sql = """
            INSERT INTO cleaned.devices
              (device_id, name, category, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s);
        """
    elif cleaned_tbl == "cleaned.smart_plugs":
        insert_sql = """
            INSERT INTO cleaned.smart_plugs
              (plug_id, user_id, room_id, device_id, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
    elif cleaned_tbl == "cleaned.plug_assignments":
        insert_sql = """
            INSERT INTO cleaned.plug_assignments
              (assignment_id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
    elif cleaned_tbl == "cleaned.readings":
        insert_sql = """
            INSERT INTO cleaned.readings
              (plug_id, "timestamp", power_kwh, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s);
        """
    elif cleaned_tbl == "cleaned.companies":
        insert_sql = """
            INSERT INTO cleaned.companies
              (company_id, name, industry, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s);
        """
    elif cleaned_tbl == "cleaned.company_locations":
        insert_sql = """
            INSERT INTO cleaned.company_locations
              (company_location_id, name, country, co2_factor, company_id, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
    elif cleaned_tbl == "cleaned.departments":
        insert_sql = """
            INSERT INTO cleaned.departments
              (department_id, company_id, name, updated_at)
            VALUES (%s, %s, %s, %s);
        """
    elif cleaned_tbl == "cleaned.company_rooms":
        insert_sql = """
            INSERT INTO cleaned.company_rooms
              (company_room_id, name, location_id, updated_at)
            VALUES (%s, %s, %s, %s);
        """
    elif cleaned_tbl == "cleaned.company_devices":
        insert_sql = """
            INSERT INTO cleaned.company_devices
              (device_id, name, category, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s);
        """
    elif cleaned_tbl == "cleaned.company_users":
        insert_sql = """
            INSERT INTO cleaned.company_users
              (user_id, company_id, name, email, department_id, role, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
    elif cleaned_tbl == "cleaned.company_smart_plugs":
        insert_sql = """
            INSERT INTO cleaned.company_smart_plugs
              (plug_id, company_id, room_id, device_id, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
    elif cleaned_tbl == "cleaned.company_plug_assignments":
        insert_sql = """
            INSERT INTO cleaned.company_plug_assignments
              (company_plug_assignment_id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
    elif cleaned_tbl == "cleaned.company_readings":
        insert_sql = """
            INSERT INTO cleaned.company_readings
              (plug_id, "timestamp", power_kwh, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s);
        """
    else:
        raise RuntimeError(f"Nije podržan INSERT za {cleaned_tbl}")

    # 4) Izvrši batch INSERT
    cur.executemany(insert_sql, cleaned_rows)
    print(f"→ Ubaceno {len(cleaned_rows):,} redova u {cleaned_tbl}.")


def main():
    print("\n=== Pokretanje populate_cleaned_python.py ===\n")

    # Spoj na db_analytical
    try:
        an_conn = psycopg2.connect(**AN_CONFIG)
    except Exception as e:
        print("Greška: ne mogu se spojiti na db_analytical:", e)
        sys.exit(1)

    cur = an_conn.cursor()

    # Iteriraj kroz sve entitete u ARCHIVE_RAW_TABLES
    for ent, info in ARCHIVE_RAW_TABLES.items():
        print(f"\n-- Popunjavanje cleaned sheme za entitet `{ent}` --")
        try:
            populate_cleaned_table(cur, info)
            an_conn.commit()
        except Exception as e:
            print(f"Greška prilikom punjenja {info['cleaned_tbl']}: {e}")
            an_conn.rollback()

    cur.close()
    an_conn.close()

    print("\n=== populate_cleaned_python.py završen! ===")


if __name__ == "__main__":
    main()
