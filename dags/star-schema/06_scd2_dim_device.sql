-- 06_scd2_dim_device.sql
-- SCD2 update za dim_device – puni iz cleaned.devices

WITH src AS (
  SELECT
    dev.device_id,
    dev.name,
    dev.category,
    dev.updated_at
  FROM cleaned.devices AS dev
)
UPDATE public.dim_device AS d
SET end_date = src.updated_at
FROM src
WHERE d.device_id = src.device_id
  AND d.end_date = '9999-12-31'
  AND (
       d.name     IS DISTINCT FROM src.name
    OR d.category IS DISTINCT FROM src.category
  );

WITH src AS (
  SELECT
    dev.device_id,
    dev.name,
    dev.category,
    dev.updated_at
  FROM cleaned.devices AS dev
)
INSERT INTO public.dim_device (
    device_id,
    name,
    category,
    start_date,
    end_date
)
SELECT
    src.device_id,
    src.name,
    src.category,
    src.updated_at AS start_date,
    '9999-12-31'::TIMESTAMP AS end_date
FROM src
LEFT JOIN public.dim_device AS d
  ON d.device_id = src.device_id
 AND d.end_date = '9999-12-31'
WHERE d.device_id IS NULL
   OR src.name     IS DISTINCT FROM d.name
   OR src.category IS DISTINCT FROM d.category;
