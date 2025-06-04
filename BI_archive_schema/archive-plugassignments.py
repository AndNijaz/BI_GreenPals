import psycopg2
from datetime import datetime
from itertools import islice

def archive_plug_assignments_incremental():
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
    process = "scd2_incremental_archive_plug_assignments"
    source = "landing.plug_assignments"

    # Dohvati najnoviji updated timestamp iz arhive
    cur.execute("SELECT MAX(updated) FROM archive.plug_assignments")
    last_updated = cur.fetchone()[0] or datetime(2000, 1, 1)
    print(f"🔍 Arhiviramo plug_assignments ažurirane nakon: {last_updated}")

    # Dohvati plug_assignments iz landing novije od last_updated
    cur.execute("""
        SELECT id, plug_id, room_id, device_id, start_time, end_time, updated_at
        FROM landing.plug_assignments
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
        for assignment_id, plug_id, room_id, device_id, start_time, end_time, updated_at in chunk:
            # Provjeri da li postoji aktivni red u arhivi
            cur.execute("""
                SELECT id, plug_id, room_id, device_id, start_time, end_time
                FROM archive.plug_assignments
                WHERE assignment_id = %s AND end_date = %s
            """, (assignment_id, future))
            archive_rows = cur.fetchall()

            if not archive_rows:
                # Novi assignment
                cur.execute("""
                    INSERT INTO archive.plug_assignments (
                        start_date, end_date, process, source, updated,
                        assignment_id, plug_id, room_id, device_id, start_time, end_time
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    updated_at, future, process, source, updated_at,
                    assignment_id, plug_id, room_id, device_id, start_time, end_time
                ))
            else:
                for row in archive_rows:
                    archive_id, old_plug, old_room, old_device, old_start, old_end = row
                    if plug_id != old_plug or room_id != old_room or device_id != old_device or start_time != old_start or end_time != old_end:
                        # Zatvori stari red
                        cur.execute("UPDATE archive.plug_assignments SET end_date = %s WHERE id = %s", (now, archive_id))
                        # Ubaci novi red
                        cur.execute("""
                            INSERT INTO archive.plug_assignments (
                                start_date, end_date, process, source, updated,
                                assignment_id, plug_id, room_id, device_id, start_time, end_time
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            updated_at, future, process, source, updated_at,
                            assignment_id, plug_id, room_id, device_id, start_time, end_time
                        ))
                        break

        conn.commit()
        print(f"✅ Obrada batcha od {len(chunk)} plug_assignments završena.")

    cur.close()
    conn.close()
    print("🎉 Incremental SCD2 arhiviranje plug_assignments završeno.")

if __name__ == "__main__":
    archive_plug_assignments_incremental()
