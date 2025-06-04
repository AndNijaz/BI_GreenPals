✅ 1. Python biblioteke
Instaliraj ako već nisi:

```
pip install requests psycopg2-binary
```

🐍 2. etl_from_api_to_landing.py

```
import requests
import psycopg2

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

# 3. Clean landing table before inserting
cur.execute("TRUNCATE landing.co2_factors")

# 4. Insert each row
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

print("✅ CO₂ faktori iz API-ja ubačeni u landing.co2_factors")
```

```

```

```

```
