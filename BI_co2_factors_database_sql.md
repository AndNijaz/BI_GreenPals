## CO2 Factors Database

```
CREATE TABLE co2_factors (
id SERIAL PRIMARY KEY,
source_name TEXT NOT NULL, -- npr. "Electricity - Bosnia", "Natural Gas", "Solar"
country TEXT NOT NULL, -- država na koju se odnosi faktor
co2_factor DECIMAL(10,5) NOT NULL, -- vrijednost faktora u kg CO2 po kWh
unit TEXT DEFAULT 'kgCO2/kWh', -- jedinica mjere
updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```
