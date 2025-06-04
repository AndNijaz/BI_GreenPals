import psycopg2
from datetime import datetime
from itertools import islice

def archive_company_users_incremental():
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
    process = "scd2_incremental_archive_company_users"
    source = "landing.company_users"

    # Dohvati najnoviji updated timestamp iz arhive
    cur.execute("SELECT MAX(updated) FROM archive.company_users")
    last_updated = cur.fetchone()[0] or datetime(2000, 1, 1)
    print(f"🔍 Arhiviramo company_users ažurirane nakon: {last_updated}")

    # Dohvati company_users iz landing novije od last_updated
    cur.execute("""
        SELECT id, company_id, name, email, department_id, role, updated_at
        FROM landing.company_users
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
        for user_id, company_id, name, email, dept_id, role, updated_at in chunk:
            # Provjeri da li postoji aktivni red u arhivi
            cur.execute("""
                SELECT id, company_id, name, email, department_id, role
                FROM archive.company_users
                WHERE user_id = %s AND end_date = %s
            """, (user_id, future))
            archive_rows = cur.fetchall()

            if not archive_rows:
                # Novi user
                cur.execute("""
                    INSERT INTO archive.company_users (
                        start_date, end_date, process, source, updated,
                        user_id, company_id, name, email, department_id, role
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    updated_at, future, process, source, updated_at,
                    user_id, company_id, name, email, dept_id, role
                ))
            else:
                for row in archive_rows:
                    archive_id, old_company, old_name, old_email, old_dept, old_role = row
                    if (company_id != old_company or name != old_name or email != old_email or
                        dept_id != old_dept or role != old_role):
                        # Zatvori stari red
                        cur.execute("UPDATE archive.company_users SET end_date = %s WHERE id = %s", (now, archive_id))
                        # Ubaci novi red
                        cur.execute("""
                            INSERT INTO archive.company_users (
                                start_date, end_date, process, source, updated,
                                user_id, company_id, name, email, department_id, role
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            updated_at, future, process, source, updated_at,
                            user_id, company_id, name, email, dept_id, role
                        ))
                        break

        conn.commit()
        print(f"✅ Obrada batcha od {len(chunk)} company_users završena.")

    cur.close()
    conn.close()
    print("🎉 Incremental SCD2 arhiviranje company_users završeno.")

if __name__ == "__main__":
    archive_company_users_incremental()
