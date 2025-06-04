import psycopg2
from datetime import datetime
from itertools import islice

def archive_users_incremental():
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
    process = "scd2_incremental_archive_users"
    source = "landing.users"

    # 1. Dohvati najnoviji timestamp iz arhive
    cur.execute("SELECT MAX(updated) FROM archive.users")
    last_updated = cur.fetchone()[0] or datetime(2000, 1, 1)
    print(f"🔍 Arhiviramo korisnike ažurirane nakon: {last_updated}")

    # 2. Dohvati korisnike iz landing zone novije od zadnjeg ažuriranja
    cur.execute("""
        SELECT id, name, email, eco_points, updated_at
        FROM landing.users
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
        for user_id, name, email, eco_points, updated_at in chunk:
            # Provjeri da li postoji aktivni red u arhivi
            cur.execute("""
                SELECT id, name, email, eco_points
                FROM archive.users
                WHERE user_id = %s AND end_date = %s
            """, (user_id, future))
            archive_rows = cur.fetchall()

            if not archive_rows:
                # Novi korisnik → insert
                cur.execute("""
                    INSERT INTO archive.users (
                        start_date, end_date, process, source, updated,
                        user_id, name, email, eco_points
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    updated_at, future, process, source, updated_at,
                    user_id, name, email, eco_points
                ))
            else:
                for row in archive_rows:
                    archive_id, old_name, old_email, old_eco = row
                    if (name != old_name) or (email != old_email) or (eco_points != old_eco):
                        # Zatvori stari red
                        cur.execute("UPDATE archive.users SET end_date = %s WHERE id = %s", (now, archive_id))
                        # Ubaci novi red
                        cur.execute("""
                            INSERT INTO archive.users (
                                start_date, end_date, process, source, updated,
                                user_id, name, email, eco_points
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            updated_at, future, process, source, updated_at,
                            user_id, name, email, eco_points
                        ))
                        break

        conn.commit()
        print(f"✅ Obrada batcha od {len(chunk)} korisnika završena.")

    cur.close()
    conn.close()
    print("🎉 Incremental SCD2 arhiviranje korisnika završeno.")

if __name__ == "__main__":
    archive_users_incremental()
