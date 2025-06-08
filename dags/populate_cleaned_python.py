# #!/usr/bin/env python3
# """
# populate_cleaned_python.py

# Popunjava cleaned.* tablice iz archive.* podataka pomoću SCD2 logike:
#   - uzima samo aktivne verzije (end_date = '9999-12-31')
#   - kod readings i company_readings koristi DISTINCT ON da izbaci duplikate
#   - za svaki entitet radi TRUNCATE + batch INSERT u cleaned shemu
# """

# import psycopg2
# import sys

# # ================================================
# # 1) Konfiguracija konekcija (samo analytical baza)
# # ================================================
# AN_CONFIG = {
#     "dbname": "db_analytical",
#     "user": "postgres",
#     "password": "napoleonlm10",
#     "host": "db_analytical",
#     "port": 5432
# }

# # ================================================
# # 2) Mapa entiteta → archive i cleaned sheme
# # ================================================
# ARCHIVE_RAW_TABLES = {
#     "users": {
#         "archive_tbl": "archive.users",
#         "cleaned_tbl": "cleaned.users",
#         "columns": ["user_id", "name", "email", "eco_points", "updated"]
#     },
#     "locations": {
#         "archive_tbl": "archive.locations",
#         "cleaned_tbl": "cleaned.locations",
#         "columns": ["location_id", "name", "country", "updated"]
#     },
#     "rooms": {
#         "archive_tbl": "archive.rooms",
#         "cleaned_tbl": "cleaned.rooms",
#         "columns": ["room_id", "name", "location_id", "updated"]
#     },
#     "devices": {
#         "archive_tbl": "archive.devices",
#         "cleaned_tbl": "cleaned.devices",
#         "columns": ["device_id", "name", "category", "updated"]
#     },
#     "smart_plugs": {
#         "archive_tbl": "archive.smart_plugs",
#         "cleaned_tbl": "cleaned.smart_plugs",
#         "columns": ["plug_id", "user_id", "room_id", "device_id", "status", "updated"]
#     },
#     "plug_assignments": {
#         "archive_tbl": "archive.plug_assignments",
#         "cleaned_tbl": "cleaned.plug_assignments",
#         "columns": ["assignment_id", "plug_id", "room_id", "device_id", "start_time", "end_time", "updated"]
#     },
#     "readings": {
#         "archive_tbl": "archive.readings",
#         "cleaned_tbl": "cleaned.readings",
#         "columns": ["plug_id", "timestamp", "power_kwh", "updated"]
#     },
#     "companies": {
#         "archive_tbl": "archive.companies",
#         "cleaned_tbl": "cleaned.companies",
#         "columns": ["company_id", "name", "industry", "updated"]
#     },
#     "company_locations": {
#         "archive_tbl": "archive.company_locations",
#         "cleaned_tbl": "cleaned.company_locations",
#         "columns": ["company_location_id", "name", "country", "co2_factor", "company_id", "updated"]
#     },
#     "departments": {
#         "archive_tbl": "archive.departments",
#         "cleaned_tbl": "cleaned.departments",
#         "columns": ["department_id", "company_id", "name", "updated"]
#     },
#     "company_rooms": {
#         "archive_tbl": "archive.company_rooms",
#         "cleaned_tbl": "cleaned.company_rooms",
#         "columns": ["company_room_id", "name", "location_id", "updated"]
#     },
#     "company_devices": {
#         "archive_tbl": "archive.company_devices",
#         "cleaned_tbl": "cleaned.company_devices",
#         "columns": ["device_id", "name", "category", "updated"]
#     },
#     "company_users": {
#         "archive_tbl": "archive.company_users",
#         "cleaned_tbl": "cleaned.company_users",
#         "columns": ["user_id", "company_id", "name", "email", "department_id", "role", "updated"]
#     },
#     "company_smart_plugs": {
#         "archive_tbl": "archive.company_smart_plugs",
#         "cleaned_tbl": "cleaned.company_smart_plugs",
#         "columns": ["plug_id", "company_id", "room_id", "device_id", "status", "updated"]
#     },
#     "company_plug_assignments": {
#         "archive_tbl": "archive.company_plug_assignments",
#         "cleaned_tbl": "cleaned.company_plug_assignments",
#         "columns": ["company_plug_assignment_id", "plug_id", "room_id", "device_id", "start_time", "end_time", "updated"]
#     },
#     "company_readings": {
#         "archive_tbl": "archive.company_readings",
#         "cleaned_tbl": "cleaned.company_readings",
#         "columns": ["plug_id", "timestamp", "power_kwh", "updated"]
#     },
#     "co2_factors": {
#         "archive_tbl": "archive.co2_factors",
#         "cleaned_tbl": "cleaned.co2_factors",
#         "columns": ["source_name", "country", "co2_factor", "unit", "updated"]
#     },
#     "electricity_prices": {
#         "archive_tbl": "archive.electricity_prices",
#         "cleaned_tbl": "cleaned.electricity_prices",
#         "columns": ["country", "price_per_kwh", "updated"]
#     },
# }

# def clean_value(val):
#     if val is None:
#         return None
#     if isinstance(val, str):
#         return val.strip()
#     return val

# def truncate_cleaned_table(cur, cleaned_tbl):
#     cur.execute(f"TRUNCATE TABLE {cleaned_tbl} RESTART IDENTITY CASCADE;")

# def populate_cleaned_table(cur, info):
#     archive_tbl = info["archive_tbl"]
#     cleaned_tbl = info["cleaned_tbl"]
#     cols = info["columns"]
#     col_list = ", ".join(cols)

#     # 1) Sastavi SQL za dohvat aktivnih verzija
#     if archive_tbl.endswith(".readings"):
#         sql = f"""
#             SELECT DISTINCT ON (plug_id, "timestamp")
#                    {col_list}
#               FROM {archive_tbl}
#              WHERE end_date = '9999-12-31'
#              ORDER BY plug_id, "timestamp", updated DESC;
#         """
#     else:
#         sql = f"SELECT {col_list} FROM {archive_tbl} WHERE end_date = '9999-12-31';"

#     cur.execute(sql)
#     rows = cur.fetchall()
#     if not rows:
#         print(f"→ Nema aktivnih redova u {archive_tbl}, preskačem {cleaned_tbl}.")
#         return

#     # 2) Mapiraj svaki row u red spreman za INSERT
#     cleaned_rows = []
#     for row in rows:
#         if cleaned_tbl == "cleaned.users":
#             uid, name, email, pts, updated = row
#             cleaned_rows.append((clean_value(uid),
#                                  clean_value(name),
#                                  clean_value(email).lower(),
#                                  clean_value(pts) or 0,
#                                  None,
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.locations":
#             lid, name, country, updated = row
#             cleaned_rows.append((clean_value(lid),
#                                  clean_value(name),
#                                  clean_value(country),
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.rooms":
#             rid, name, loc_id, updated = row
#             cleaned_rows.append((clean_value(rid),
#                                  clean_value(name),
#                                  clean_value(loc_id),
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.devices":
#             did, name, cat, updated = row
#             cleaned_rows.append((clean_value(did),
#                                  clean_value(name),
#                                  clean_value(cat),
#                                  None,
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.smart_plugs":
#             pid, uid, rid, did, st, updated = row
#             cleaned_rows.append((clean_value(pid),
#                                  clean_value(uid),
#                                  clean_value(rid),
#                                  clean_value(did),
#                                  clean_value(st),
#                                  None,
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.plug_assignments":
#             aid, pid, rid, did, stime, etime, updated = row
#             cleaned_rows.append((clean_value(aid),
#                                  clean_value(pid),
#                                  clean_value(rid),
#                                  clean_value(did),
#                                  clean_value(stime),
#                                  clean_value(etime),
#                                  None,
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.readings":
#             pid, ts, pwr, updated = row
#             cleaned_rows.append((clean_value(pid),
#                                  clean_value(ts),
#                                  clean_value(pwr),
#                                  None,
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.companies":
#             cid, name, ind, updated = row
#             cleaned_rows.append((clean_value(cid),
#                                  clean_value(name),
#                                  clean_value(ind),
#                                  None,
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.company_locations":
#             clid, name, country, co2, cid, updated = row
#             cleaned_rows.append((clean_value(clid),
#                                  clean_value(name),
#                                  clean_value(country),
#                                  clean_value(co2),
#                                  clean_value(cid),
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.departments":
#             did, cid, name, updated = row
#             cleaned_rows.append((clean_value(did),
#                                  clean_value(cid),
#                                  clean_value(name),
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.company_rooms":
#             crid, name, loc_id, updated = row
#             cleaned_rows.append((clean_value(crid),
#                                  clean_value(name),
#                                  clean_value(loc_id),
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.company_devices":
#             did, name, cat, updated = row
#             cleaned_rows.append((clean_value(did),
#                                  clean_value(name),
#                                  clean_value(cat),
#                                  None,
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.company_users":
#             uid, cid, name, email, dept, role, updated = row
#             cleaned_rows.append((clean_value(uid),
#                                  clean_value(cid),
#                                  clean_value(name),
#                                  clean_value(email).lower(),
#                                  clean_value(dept),
#                                  clean_value(role),
#                                  None,
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.company_smart_plugs":
#             pid, cid, rid, did, st, updated = row
#             cleaned_rows.append((clean_value(pid),
#                                  clean_value(cid),
#                                  clean_value(rid),
#                                  clean_value(did),
#                                  clean_value(st),
#                                  None,
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.company_plug_assignments":
#             cpaid, pid, rid, did, stime, etime, updated = row
#             cleaned_rows.append((clean_value(cpaid),
#                                  clean_value(pid),
#                                  clean_value(rid),
#                                  clean_value(did),
#                                  clean_value(stime),
#                                  clean_value(etime),
#                                  None,
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.company_readings":
#             pid, ts, pwr, updated = row
#             cleaned_rows.append((clean_value(pid),
#                                  clean_value(ts),
#                                  clean_value(pwr),
#                                  None,
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.co2_factors":
#             src, country, factor, unit, updated = row
#             cleaned_rows.append((clean_value(src),
#                                  clean_value(country),
#                                  clean_value(factor),
#                                  clean_value(unit),
#                                  clean_value(updated)))
#         elif cleaned_tbl == "cleaned.electricity_prices":
#             country, price, updated = row
#             cleaned_rows.append((clean_value(country),
#                                  clean_value(price),
#                                  clean_value(updated)))
#         else:
#             raise RuntimeError(f"Nepoznat cleaned_tbl: {cleaned_tbl}")

#     # 3) TRUNCATE + batch INSERT
#     truncate_cleaned_table(cur, cleaned_tbl)

#     # Odredi INSERT SQL prema tablici
#     insert_sql_map = {
#         "cleaned.users": """
#             INSERT INTO cleaned.users
#               (user_id, name, email, eco_points, created_at, updated_at)
#             VALUES (%s, %s, %s, %s, %s, %s);
#         """,
#         "cleaned.locations": """
#             INSERT INTO cleaned.locations
#               (location_id, name, country, updated_at)
#             VALUES (%s, %s, %s, %s);
#         """,
#         "cleaned.rooms": """
#             INSERT INTO cleaned.rooms
#               (room_id, name, location_id, updated_at)
#             VALUES (%s, %s, %s, %s);
#         """,
#         "cleaned.devices": """
#             INSERT INTO cleaned.devices
#               (device_id, name, category, created_at, updated_at)
#             VALUES (%s, %s, %s, %s, %s);
#         """,
#         "cleaned.smart_plugs": """
#             INSERT INTO cleaned.smart_plugs
#               (plug_id, user_id, room_id, device_id, status, created_at, updated_at)
#             VALUES (%s, %s, %s, %s, %s, %s, %s);
#         """,
#         "cleaned.plug_assignments": """
#             INSERT INTO cleaned.plug_assignments
#               (assignment_id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
#         """,
#         "cleaned.readings": """
#             INSERT INTO cleaned.readings
#               (plug_id, "timestamp", power_kwh, created_at, updated_at)
#             VALUES (%s, %s, %s, %s, %s);
#         """,
#         "cleaned.companies": """
#             INSERT INTO cleaned.companies
#               (company_id, name, industry, created_at, updated_at)
#             VALUES (%s, %s, %s, %s, %s);
#         """,
#         "cleaned.company_locations": """
#             INSERT INTO cleaned.company_locations
#               (company_location_id, name, country, co2_factor, company_id, updated_at)
#             VALUES (%s, %s, %s, %s, %s, %s);
#         """,
#         "cleaned.departments": """
#             INSERT INTO cleaned.departments
#               (department_id, company_id, name, updated_at)
#             VALUES (%s, %s, %s, %s);
#         """,
#         "cleaned.company_rooms": """
#             INSERT INTO cleaned.company_rooms
#               (company_room_id, name, location_id, updated_at)
#             VALUES (%s, %s, %s, %s);
#         """,
#         "cleaned.company_devices": """
#             INSERT INTO cleaned.company_devices
#               (device_id, name, category, created_at, updated_at)
#             VALUES (%s, %s, %s, %s, %s);
#         """,
#         "cleaned.company_users": """
#             INSERT INTO cleaned.company_users
#               (user_id, company_id, name, email, department_id, role, created_at, updated_at)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
#         """,
#         "cleaned.company_smart_plugs": """
#             INSERT INTO cleaned.company_smart_plugs
#               (plug_id, company_id, room_id, device_id, status, created_at, updated_at)
#             VALUES (%s, %s, %s, %s, %s, %s, %s);
#         """,
#         "cleaned.company_plug_assignments": """
#             INSERT INTO cleaned.company_plug_assignments
#               (company_plug_assignment_id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
#         """,
#         "cleaned.company_readings": """
#             INSERT INTO cleaned.company_readings
#               (plug_id, "timestamp", power_kwh, created_at, updated_at)
#             VALUES (%s, %s, %s, %s, %s);
#         """,
#         "cleaned.co2_factors": """
#             INSERT INTO cleaned.co2_factors
#               (source_name, country, co2_factor, unit, updated_at)
#             VALUES (%s, %s, %s, %s, %s);
#         """,
#         "cleaned.electricity_prices": """
#             INSERT INTO cleaned.electricity_prices
#               (country, price_per_kwh, updated_at)
#             VALUES (%s, %s, %s);
#         """,
#     }
#     insert_sql = insert_sql_map.get(cleaned_tbl)
#     if not insert_sql:
#         raise RuntimeError(f"Nije podržan INSERT za {cleaned_tbl}")

#     cur.executemany(insert_sql, cleaned_rows)
#     print(f"→ Ubaceno {len(cleaned_rows):,} redova u {cleaned_tbl}.")

# def main():
#     print("\n=== Pokretanje populate_cleaned_python.py ===\n")
#     try:
#         conn = psycopg2.connect(**AN_CONFIG)
#     except Exception as e:
#         print("Greška: ne mogu se spojiti na db_analytical:", e)
#         sys.exit(1)

#     cur = conn.cursor()
#     for ent, info in ARCHIVE_RAW_TABLES.items():
#         print(f"\n-- Popunjavanje {info['cleaned_tbl']} --")
#         try:
#             populate_cleaned_table(cur, info)
#             conn.commit()
#         except Exception as e:
#             print(f"Greška prilikom punjenja {info['cleaned_tbl']}: {e}")
#             conn.rollback()

#     cur.close()
#     conn.close()
#     print("\n=== populate_cleaned_python.py završen! ===")

# if __name__ == "__main__":
#     main()


#!/usr/bin/env python3
import psycopg2
import sys

# ===============================
# 1) Konekcija na analytical bazu
# ===============================
AN_CONFIG = {
    "dbname": "db_analytical",
    "user": "postgres",
    "password": "napoleonlm10",
    "host": "db_analytical",
    "port": 5432
}

# =====================================================
# 2) Mapa entiteta → archive.* i cleaned.* tablice
# =====================================================
ARCHIVE_RAW_TABLES = {
    # ... ostali entiteti ...
    "company_readings": {
        "archive_tbl": "archive.company_readings",
        "cleaned_tbl": "cleaned.company_readings",
        "columns": ["plug_id", "timestamp", "power_kwh", "updated"]
    },
    # ... co2_factors, electricity_prices, itd. ...
}

def clean_value(val):
    if val is None:
        return None
    if isinstance(val, str):
        return val.strip()
    return val

def truncate_cleaned_table(cur, cleaned_tbl):
    cur.execute(f"TRUNCATE TABLE {cleaned_tbl} RESTART IDENTITY CASCADE;")

def populate_cleaned_table(cur, info):
    archive_tbl = info["archive_tbl"]
    cleaned_tbl = info["cleaned_tbl"]
    cols = info["columns"]
    col_list = ", ".join(cols)

    # ---- za readings (i company_readings) koristimo DISTINCT ON ----
    if cleaned_tbl in ("cleaned.readings", "cleaned.company_readings"):
        sql = f"""
            SELECT DISTINCT ON (plug_id, "timestamp")
                   {col_list}
              FROM {archive_tbl}
             WHERE end_date = '9999-12-31'
             ORDER BY plug_id, "timestamp", updated DESC;
        """
    else:
        sql = f"""
            SELECT {col_list}
              FROM {archive_tbl}
             WHERE end_date = '9999-12-31';
        """

    cur.execute(sql)
    rows = cur.fetchall()
    if not rows:
        print(f"→ Nema aktivnih redova u {archive_tbl}, preskačem {cleaned_tbl}.")
        return

    cleaned_rows = []
    for row in rows:
        if cleaned_tbl == "cleaned.company_readings":
            plug_id, ts, power, updated = row
            cleaned_rows.append((
                clean_value(plug_id),
                clean_value(ts),
                clean_value(power),
                None,               # created_at
                clean_value(updated)  # updated_at
            ))
        # ovdje idu ostali entiteti (readings, users, co2_factors, itd.)
        # ...

    # truncate & insert
    truncate_cleaned_table(cur, cleaned_tbl)

    # odabir INSERT‐SQL
    if cleaned_tbl == "cleaned.company_readings":
        insert_sql = """
            INSERT INTO cleaned.company_readings
              (plug_id, "timestamp", power_kwh, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s);
        """
    # elif cleaned_tbl == "cleaned.readings": ...
    # elif ... ostali entiteti
    else:
        raise RuntimeError(f"Nije podržan INSERT za {cleaned_tbl}")

    cur.executemany(insert_sql, cleaned_rows)
    print(f"→ Ubaceno {len(cleaned_rows):,} redova u {cleaned_tbl}.")

def main():
    try:
        conn = psycopg2.connect(**AN_CONFIG)
    except Exception as e:
        print("Ne mogu se spojiti na db_analytical:", e)
        sys.exit(1)

    cur = conn.cursor()
    for ent, info in ARCHIVE_RAW_TABLES.items():
        print(f"\n-- Popunjavam `{info['cleaned_tbl']}` iz `{info['archive_tbl']}` --")
        try:
            populate_cleaned_table(cur, info)
            conn.commit()
        except Exception as e:
            print(f"Greška pri punjenju {info['cleaned_tbl']}: {e}")
            conn.rollback()

    cur.close()
    conn.close()
    print("\n=== Gotovo populate_cleaned_python.py ===")

if __name__ == "__main__":
    main()
