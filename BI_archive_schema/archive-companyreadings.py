import psycopg2
from datetime import datetime
from itertools import islice

def archive_company_readings_incremental():
    conn = psycopg2.connect(
        dbname="BI_Users",
        user="postgres",
        password="napoleonlm10",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    now = datetime.now()
    future = datetime(9999, 12, 31)
    process = "scd2_incremental_archive_company_readings"
    source = "landing.company_readings"

    # Dohvati najnoviji updated timestamp iz arhive
    cur.execute("SELECT MAX(updated) FROM archive.company_readings")
    last_updated = cur.fetchone()[0] or datetime(2000, 1, 1)
    print(f"🔍 Arhiviramo company_readings ažurirane nakon: {last_updated}")

    # Dohvati company_readings iz landing novije od last_updated
    cur.execute("""
        SELECT id, plug_id, timestamp, power_kwh, updated_at
        FROM landing.company_readings
        WHERE updated_at > %s
    """, (last_updated,))
    rows = cur.fetchall()

    def batch(iterable, size=1000):
        it = iter(iterable)
        while True:
            chunk = list(islice(it, size))
            if not chunk:
                break
            yield chunk

    for chunk in batch(rows):
        for reading_id, plug_id, ts, power_kwh, updated_at in chunk:
            # Provjeri postoji li aktivni red za ovo plug_id + timestamp
            cur.execute("""
                SELECT id, power_kwh FROM archive.company_readings
                WHERE plug_id = %s AND timestamp = %s AND end_date = %s
            """, (plug_id, ts, future))
            archive_rows = cur.fetchall()

            if not archive_rows:
                # Novi reading
                cur.execute("""
                    INSERT INTO archive.company_readings (
                        start_date, end_date, process, source, updated,
                        plug_id, timestamp, power_kwh
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    updated_at, future, process, source, updated_at,
                    plug_id, ts, power_kwh
                ))
            else:
                for row in archive_rows:
                    archive_id, old_power = row
                    if float(power_kwh) != float(old_power):
                        # Zatvori stari red
                        cur.execute("UPDATE archive.company_readings SET end_date = %s WHERE id = %s", (now, archive_id))
                        # Ubaci novi red
                        cur.execute("""
                            INSERT INTO archive.company_readings (
                                start_date, end_date, process, source, updated,
                                plug_id, timestamp, power_kwh
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            updated_at, future, process, source, updated_at,
                            plug_id, ts, power_kwh
                        ))
                        break

        conn.commit()
        print(f"✅ Obrada batcha od {len(chunk)} company_readings završena.")

    cur.close()
    conn.close()
    print("🎉 Incremental SCD2 arhiviranje company_readings završeno.")

if __name__ == "__main__":
    archive_company_readings_incremental()
