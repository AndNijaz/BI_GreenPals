from fastapi import FastAPI, Query
from typing import Optional
import psycopg

app = FastAPI()

# Use a URI so psycopg3 won't try to load any pgpass file
conn = psycopg.connect("postgresql://postgres:napoleonlm10@localhost:5432/BI_co2_factor")

@app.get("/co2-factors")
def get_co2_factors(
    country: Optional[str] = Query(None),
    source_name: Optional[str] = Query(None)
):
    cur = conn.cursor()
    query = (
        "SELECT source_name, country, co2_factor, unit, updated_at "
        "FROM co2_factors WHERE TRUE"
    )
    params = []
    if country:
        query += " AND country ILIKE %s"
        params.append(f"%{country}%")
    if source_name:
        query += " AND source_name ILIKE %s"
        params.append(f"%{source_name}%")

    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()

    return [
        {
            "source_name": r[0],
            "country": r[1],
            "co2_factor": float(r[2]),
            "unit": r[3],
            "updated_at": r[4].isoformat()
        }
        for r in rows
    ]
