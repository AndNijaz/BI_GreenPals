from fastapi import FastAPI, Query
from typing import Optional
import psycopg

app = FastAPI()

conn = psycopg.connect("postgresql://postgres:napoleonlm10@localhost:5432/BI_electricity_prices")

@app.get("/electricity-prices")
def get_electricity_prices(
    country: Optional[str] = Query(None)
):
    cur = conn.cursor()
    query = (
        "SELECT country, price_per_kwh FROM electricity_prices WHERE TRUE"
    )
    params = []
    if country:
        query += " AND country ILIKE %s"
        params.append(f"%{country}%")

    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()

    return [
        {
            "country": r[0],
            "price_per_kwh": float(r[1])
        }
        for r in rows
    ]
