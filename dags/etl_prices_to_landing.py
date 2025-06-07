import os, requests, psycopg2

def main():
    resp = requests.get("http://price-api:8001/electricity-prices")
    data = resp.json()

    conn = psycopg2.connect(
      dbname="db_analytical", user="postgres",
      password="napoleonlm10", host="db_analytical", port=5432
    )
    cur = conn.cursor()
    cur.execute("TRUNCATE landing.electricity_prices")
    for r in data:
        cur.execute("""
          INSERT INTO landing.electricity_prices
            (country,price_per_kwh,updated_at)
          VALUES (%s,%s,CURRENT_TIMESTAMP)
          ON CONFLICT(country) DO UPDATE
            SET price_per_kwh=EXCLUDED.price_per_kwh,
                updated_at=EXCLUDED.updated_at
        """, (r["country"], r["price_per_kwh"]))
    conn.commit()
    cur.close(); conn.close()
    print("✅ electricity_prices → landing done")

if __name__=="__main__":
    main()
