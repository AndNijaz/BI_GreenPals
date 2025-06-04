📦 1. main.py (nastavak ili novi fajl)

```
from fastapi import FastAPI, Query
from typing import Optional
import psycopg2

app = FastAPI()

# Konekcija na bazu
conn = psycopg2.connect(
    dbname="BI_electircity_prices",
    user="postgres",
    password="napoleonlm10",
    host="localhost",
    port=5432
)

@app.get("/electricity-prices")
def get_electricity_prices(country: Optional[str] = Query(None)):
    cur = conn.cursor()
    query = "SELECT country, price_per_kwh FROM electricity_prices WHERE TRUE"
    params = []

    if country:
        query += " AND country ILIKE %s"
        params.append(f"%{country}%")

    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()

    return [
        {"country": r[0], "price_per_kwh": float(r[1])}
        for r in rows
    ]

```

▶️ 2. Pokreni server:

```
python -m uvicorn ep_api:app --host 0.0.0.0 --port 8001 --reload
```

Otvori u browseru:

```
http://localhost:8001/electricity-prices
```

```
http://localhost:8001/electricity-prices?country=France
```
