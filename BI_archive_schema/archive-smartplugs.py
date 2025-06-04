import psycopg2
from datetime import datetime
from itertools import islice

def archive_smart_plugs_incremental():
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
    process = "scd2_incremental_archive_smart_plugs"
    source = "landing.smart_plugs"

    # Dohvati najnoviji updated timestamp iz arhive
    cur.execute("SELECT MAX(updated) FROM archive.smart_plugs")
    last_updated = cur.fetchone()[0] or datetime(2000, 1, 1)
    print(f"🔍 Arhiviramo smart_plugs ažurirane nakon: {last_updated}")

    # Dohvati smart_plugs iz landing novije od last_updated
    cur.execute("""
        SELECT id, user_id, room_id, device_id, status, updated_at
        FROM landing.smart_plugs
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
        for plug_id, user_id, room_id, device_id, status, updated_at in chunk:
            # Provjeri da li postoji aktivni red u arhivi
            cur.execute("""
                SELECT id, user_id, room_id, device_id, status
                FROM archive.smart_plugs
                WHERE plug_id = %s AND end_date = %s
            """, (plug_id, future))
            archive_rows = cur.fetchall()

            if not archive_rows:
                # Novi smart plug
                cur.execute("""
                    INSERT INTO archive.smart_plugs (
                        start_date, end_date, process, source, updated,
                        plug_id, user_id, room_id, device_id, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    updated_at, future, process, source, updated_at,
                    plug_id, user_id, room_id, device_id, status
                ))
            else:
                for row in archive_rows:
                    archive_id, old_user, old_room, old_device, old_status = row
                    if user_id != old_user or room_id != old_room or device_id != old_device or status != old_status:
                        # Zatvori stari red
                        cur.execute("UPDATE archive.smart_plugs SET end_date = %s WHERE id = %s", (now, archive_id))
                        # Ubaci novi red
                        cur.execute("""
                            INSERT INTO archive.smart_plugs (
                                start_date, end_date, process, source, updated,
                                plug_id, user_id, room_id, device_id, status
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            updated_at, future, process, source, updated_at,
                            plug_id, user_id, room_id, device_id, status
                        ))
                        break

        conn.commit()
        print(f"✅ Obrada batcha od {len(chunk)} smart_plugs završena.")

    cur.close()
    conn.close()
    print("🎉 Incremental SCD2 arhiviranje smart_plugs završeno.")

if __name__ == "__main__":
    archive_smart_plugs_incremental()
