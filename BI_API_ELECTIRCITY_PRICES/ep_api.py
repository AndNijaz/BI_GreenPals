from fastapi import FastAPI, Query
from typing import Optional
import psycopg2

# 1) Inicijalizacija FastAPI app instance
app = FastAPI()

# 2) Podesite konekciju prema bazi (ostavljam vaš postojeći kod)
conn = psycopg2.connect(
    dbname="BI_electricity_prices",
    user="postgres",
    password="napoleonlm10",
    host="localhost",
    port=5432
)

# 3) Definišite GET endpoint
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
