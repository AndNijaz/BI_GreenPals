import psycopg2
from datetime import datetime
from itertools import islice

def archive_devices_incremental():
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
    process = "scd2_incremental_archive_devices"
    source = "landing.devices"

    # Dohvati najnoviji updated timestamp iz arhive
    cur.execute("SELECT MAX(updated) FROM archive.devices")
    last_updated = cur.fetchone()[0] or datetime(2000, 1, 1)
    print(f"🔍 Arhiviramo devices ažurirane nakon: {last_updated}")

    # Dohvati devices iz landing novije od last_updated
    cur.execute("""
        SELECT id, name, category, updated_at
        FROM landing.devices
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
        for device_id, name, category, updated_at in chunk:
            # Provjeri da li postoji aktivni red u arhivi
            cur.execute("""
                SELECT id, name, category
                FROM archive.devices
                WHERE device_id = %s AND end_date = %s
            """, (device_id, future))
            archive_rows = cur.fetchall()

            if not archive_rows:
                # Novi device
                cur.execute("""
                    INSERT INTO archive.devices (
                        start_date, end_date, process, source, updated,
                        device_id, name, category
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    updated_at, future, process, source, updated_at,
                    device_id, name, category
                ))
            else:
                for row in archive_rows:
                    archive_id, old_name, old_category = row
                    if name != old_name or category != old_category:
                        # Zatvori stari red
                        cur.execute("UPDATE archive.devices SET end_date = %s WHERE id = %s", (now, archive_id))
                        # Ubaci novi red
                        cur.execute("""
                            INSERT INTO archive.devices (
                                start_date, end_date, process, source, updated,
                                device_id, name, category
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            updated_at, future, process, source, updated_at,
                            device_id, name, category
                        ))
                        break

        conn.commit()
        print(f"✅ Obrada batcha od {len(chunk)} devices završena.")

    cur.close()
    conn.close()
    print("🎉 Incremental SCD2 arhiviranje devices završeno.")

if __name__ == "__main__":
    archive_devices_incremental()
