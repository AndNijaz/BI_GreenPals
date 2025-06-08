-- 04_scd2_dim_location.sql
-- SCD2 update za dim_location – puni iz cleaned.locations

-- 1) Zatvorimo stare verzije
UPDATE public.dim_location AS d
SET end_date = c.updated_at
FROM cleaned.locations AS c
WHERE d.location_id = c.location_id
  AND d.end_date = '9999-12-31'
  AND (
       d.name    IS DISTINCT FROM c.name
    OR d.country IS DISTINCT FROM c.country
  );

-- 2) Ubacimo nove verzije
INSERT INTO public.dim_location (
    location_id, name, country, start_date, end_date
)
SELECT
    c.location_id,
    c.name,
    c.country,
    c.updated_at AS start_date,
    '9999-12-31'::TIMESTAMP AS end_date
FROM cleaned.locations AS c
LEFT JOIN public.dim_location AS d
  ON d.location_id = c.location_id
 AND d.end_date = '9999-12-31'
WHERE d.location_id IS NULL
   OR c.name    IS DISTINCT FROM d.name
   OR c.country IS DISTINCT FROM d.country;
