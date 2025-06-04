🐍 etl_prices_to_landing.py

```
import requests
import psycopg2
from psycopg2 import sql

try:
    # 1. Fetch from API
    url = "http://localhost:8001/electricity-prices"
    response = requests.get(url)
    data = response.json()

    # 2. Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="BI_Users",
        user="postgres",
        password="napoleonlm10",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    # 3. Ensure schema and table exist
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS landing;

        CREATE TABLE IF NOT EXISTS landing.electricity_prices (
            country TEXT PRIMARY KEY,
            price_per_kwh DECIMAL(6,3) NOT NULL
        );
    """)
    conn.commit()

    # 4. Truncate old data
    cur.execute("TRUNCATE landing.electricity_prices")

    # 5. Insert new data
    for item in data:
        cur.execute("""
            INSERT INTO landing.electricity_prices (country, price_per_kwh)
            VALUES (%s, %s)
            ON CONFLICT (country) DO UPDATE SET price_per_kwh = EXCLUDED.price_per_kwh;
        """, (item["country"], item["price_per_kwh"]))

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Cijene struje ubačene u landing.electricity_prices")

except Exception as e:
    print("🔥 Došlo je do greške:")
    print(e)

```
