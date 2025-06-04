import psycopg2
from datetime import datetime
from itertools import islice

def archive_companies_incremental():
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
    process = "scd2_incremental_archive_companies"
    source = "landing.companies"

    # Dohvati najnoviji updated timestamp iz arhive
    cur.execute("SELECT MAX(updated) FROM archive.companies")
    last_updated = cur.fetchone()[0] or datetime(2000, 1, 1)
    print(f"🔍 Arhiviramo companies ažurirane nakon: {last_updated}")

    # Dohvati companies iz landing novije od last_updated
    cur.execute("""
        SELECT id, name, industry, updated_at
        FROM landing.companies
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
        for company_id, name, industry, updated_at in chunk:
            # Provjeri da li postoji aktivni red u arhivi
            cur.execute("""
                SELECT id, name, industry
                FROM archive.companies
                WHERE company_id = %s AND end_date = %s
            """, (company_id, future))
            archive_rows = cur.fetchall()

            if not archive_rows:
                # Novi red
                cur.execute("""
                    INSERT INTO archive.companies (
                        start_date, end_date, process, source, updated,
                        company_id, name, industry
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    updated_at, future, process, source, updated_at,
                    company_id, name, industry
                ))
            else:
                for row in archive_rows:
                    archive_id, old_name, old_industry = row
                    if name != old_name or industry != old_industry:
                        # Zatvori stari red
                        cur.execute("UPDATE archive.companies SET end_date = %s WHERE id = %s", (now, archive_id))
                        # Ubaci novi red
                        cur.execute("""
                            INSERT INTO archive.companies (
                                start_date, end_date, process, source, updated,
                                company_id, name, industry
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            updated_at, future, process, source, updated_at,
                            company_id, name, industry
                        ))
                        break

        conn.commit()
        print(f"✅ Obrada batcha od {len(chunk)} companies završena.")

    cur.close()
    conn.close()
    print("🎉 Incremental SCD2 arhiviranje companies završeno.")

if __name__ == "__main__":
    archive_companies_incremental()
