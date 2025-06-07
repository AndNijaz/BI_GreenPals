-- 02_populate_dim_time.sql
-- Ovaj SQL koristi generate_series da izgradi dim_time za svaku minutu
-- u zadanom rasponu. Podesi raspon po potrebi.

-- Primjer: pokriva razdoblje od 2022-01-01 00:00 do 2025-12-31 23:59
WITH times AS (
  SELECT 
    gs AS ts
  FROM generate_series(
         '2022-01-01 00:00:00'::timestamp,
         '2025-12-31 23:59:00'::timestamp,
         INTERVAL '1 minute'
       ) AS gs
)
INSERT INTO public.dim_time (
  full_date,
  year,
  quarter,
  month,
  month_name,
  day,
  day_of_week,
  hour,
  minute,
  is_weekend
)
SELECT
  ts::date AS full_date,
  EXTRACT(YEAR FROM ts)::INTEGER       AS year,
  EXTRACT(QUARTER FROM ts)::INTEGER    AS quarter,
  EXTRACT(MONTH FROM ts)::INTEGER      AS month,
  TO_CHAR(ts, 'Month')                 AS month_name,
  EXTRACT(DAY FROM ts)::INTEGER        AS day,
  EXTRACT(DOW FROM ts)::INTEGER        AS day_of_week,   -- 0=nedjelja,…,6=subota
  EXTRACT(HOUR FROM ts)::INTEGER       AS hour,
  EXTRACT(MINUTE FROM ts)::INTEGER     AS minute,
  (EXTRACT(DOW FROM ts) IN (0,6))      AS is_weekend
FROM times
ON CONFLICT (time_id) DO NOTHING;
