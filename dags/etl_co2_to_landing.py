import os, requests, psycopg2

def main():
    # 1) Fetch
    resp = requests.get("http://co2-api:8000/co2-factors")
    data = resp.json()

    # 2) Connect
    conn = psycopg2.connect(
      dbname="db_analytical", user="postgres",
      password="napoleonlm10", host="db_analytical", port=5432
    )
    cur = conn.cursor()

    # 3) Truncate + insert
    cur.execute("TRUNCATE landing.co2_factors")
    for r in data:
        cur.execute("""
          INSERT INTO landing.co2_factors
            (source_name,country,co2_factor,unit,updated_at)
          VALUES (%s,%s,%s,%s,%s)
        """, (r["source_name"],r["country"],r["co2_factor"],r["unit"],r["updated_at"]))
    conn.commit()
    cur.close(); conn.close()
    print("✅ co2 → landing done")

if __name__=="__main__":
    main()
