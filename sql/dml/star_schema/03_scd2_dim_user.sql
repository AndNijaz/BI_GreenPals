-- 03_scd2_dim_user.sql
-- SCD2 update za dim_user – puni iz cleaned.users

-- 1) Zatvaramo (end_date) stare retke ako se promijenio bilo koji atribut
UPDATE public.dim_user AS d
SET end_date = c.updated_at
FROM cleaned.users AS c
WHERE d.user_id = c.user_id
  AND d.end_date = '9999-12-31'
  AND (
       d.name       IS DISTINCT FROM c.name
    OR d.email      IS DISTINCT FROM c.email
    OR d.eco_points IS DISTINCT FROM c.eco_points
  );

-- 2) Ubacujemo nove ili izmijenjene retke
INSERT INTO public.dim_user (
    user_id, name, email, eco_points, start_date, end_date
)
SELECT
    c.user_id,
    c.name,
    c.email,
    c.eco_points,
    c.updated_at AS start_date,
    '9999-12-31'::TIMESTAMP AS end_date
FROM cleaned.users AS c
LEFT JOIN public.dim_user AS d
  ON d.user_id = c.user_id
 AND d.end_date = '9999-12-31'
WHERE d.user_id IS NULL
   OR c.name       IS DISTINCT FROM d.name
   OR c.email      IS DISTINCT FROM d.email
   OR c.eco_points IS DISTINCT FROM d.eco_points;
