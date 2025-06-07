-- 05_scd2_dim_room.sql

-- 1) Pripremimo ugniježđeni SELECT da dobijemo odgovarajući location_key
WITH src AS (
  SELECT
    c.room_id,
    c.name,
    dl.location_key,
    c.updated_at
  FROM cleaned.rooms AS c
  JOIN public.dim_location AS dl
    ON dl.location_id = c.location_id
   AND dl.end_date = '9999-12-31'
)
-- Zatvorimo stare verzije (UPDATE)
UPDATE public.dim_room AS d
SET end_date = src.updated_at
FROM src
WHERE d.room_id = src.room_id
  AND d.end_date = '9999-12-31'
  AND (
       d.name        IS DISTINCT FROM src.name
    OR d.location_key IS DISTINCT FROM src.location_key
  );

-- 2) Ponovo koristimo CTE za INSERT (moramo deklarirati `WITH src` opet)
WITH src AS (
  SELECT
    c.room_id,
    c.name,
    dl.location_key,
    c.updated_at
  FROM cleaned.rooms AS c
  JOIN public.dim_location AS dl
    ON dl.location_id = c.location_id
   AND dl.end_date = '9999-12-31'
)
INSERT INTO public.dim_room (
    room_id, name, location_key, start_date, end_date
)
SELECT
    src.room_id,
    src.name,
    src.location_key,
    src.updated_at AS start_date,
    '9999-12-31'::TIMESTAMP AS end_date
FROM src
LEFT JOIN public.dim_room AS d
  ON d.room_id = src.room_id
 AND d.end_date = '9999-12-31'
WHERE d.room_id IS NULL
   OR src.name        IS DISTINCT FROM d.name
   OR src.location_key IS DISTINCT FROM d.location_key;
