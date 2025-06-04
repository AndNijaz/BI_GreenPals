Instaliraj FastAPI i uvicorn

```
pip install fastapi uvicorn psycopg2-binary
```

main.py – API servis

```
from fastapi import FastAPI, Query
from typing import List, Optional
import psycopg2
import os

app = FastAPI()

# DB konekcija
conn = psycopg2.connect(
    dbname="BI_co2_factors",
    user="postgres",
    password="napoleonlm10",
    host="localhost",
    port=5432
)

@app.get("/co2-factors")
def get_co2_factors(
    country: Optional[str] = Query(None),
    source_name: Optional[str] = Query(None)
):
    cur = conn.cursor()
    query = "SELECT source_name, country, co2_factor, unit, updated_at FROM co2_factors WHERE TRUE"
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
```

▶️ 3. Pokreni server:

```
python -m uvicorn co2-api:app --reload
```

Otvori u browseru:

```
http://127.0.0.1:8000/co2-factors
```

Ili filtrirano:

```
http://127.0.0.1:8000/co2-factors?country=Bosnia
```
