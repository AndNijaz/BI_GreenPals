```
-- Insert podataka
INSERT INTO electricity_prices (country, price_per_kwh) VALUES
('Germany', 0.37),
('Bosnia and Herzegovina', 0.12),
('Sweden', 0.26),
('France', 0.22),
('Poland', 0.30),
('USA', 0.16),
('Norway', 0.18),
('Croatia', 0.14),
('Serbia', 0.15),
('Slovenia', 0.17)
ON CONFLICT (country) DO UPDATE SET price_per_kwh = EXCLUDED.price_per_kwh;

```
