```
CREATE SCHEMA IF NOT EXISTS public;

-- Kreiraj tabelu za cijene struje
CREATE TABLE IF NOT EXISTS electricity_prices (
country TEXT PRIMARY KEY,
price_per_kwh DECIMAL(6,3) NOT NULL
);

```
