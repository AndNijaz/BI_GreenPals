import requests
import psycopg2
from psycopg2 import sql, errors

try:
    # 1. Fetch from API
    url = "http://localhost:8000/co2-factors"
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

    # 3. Check if table exists
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'landing'
              AND table_name   = 'co2_factors'
        );
    """)
    exists = cur.fetchone()[0]

    if not exists:
        print("🚨 Table landing.co2_factors does not exist! Creating it now...")
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS landing;
            CREATE TABLE landing.co2_factors (
                source_name TEXT NOT NULL,
                country TEXT NOT NULL,
                co2_factor DECIMAL(10,5) NOT NULL,
                unit TEXT DEFAULT 'kgCO2/kWh',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        print("✅ Table created.")

    # 4. Truncate table before load
    cur.execute("TRUNCATE landing.co2_factors")

    # 5. Insert data
    for item in data:
        cur.execute("""
            INSERT INTO landing.co2_factors (source_name, country, co2_factor, unit, updated_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            item["source_name"],
            item["country"],
            item["co2_factor"],
            item["unit"],
            item["updated_at"]
        ))

    conn.commit()
    cur.close()
    conn.close()

    print("✅ CO₂ faktori ubačeni u landing.co2_factors")

except (psycopg2.Error, Exception) as e:
    print("🔥 Greška prilikom izvršavanja ETL procesa:")
    print(e)
