-- 08_scd2_dim_department.sql
-- SCD2 update za dim_department – puni iz cleaned.departments

-- 1) Prvo zatvorimo postojeće (“end_date”) verzije koje su se promijenile
WITH src AS (
  SELECT
    dpt.department_id,
    dpt.name,
    dc.company_key,
    dpt.updated_at
  FROM cleaned.departments AS dpt
  JOIN public.dim_company AS dc
    ON dc.company_id = dpt.company_id
   AND dc.end_date = '9999-12-31'
)
UPDATE public.dim_department AS d
SET end_date = src.updated_at
FROM src
WHERE d.department_id = src.department_id
  AND d.end_date = '9999-12-31'
  AND (
       d.name       IS DISTINCT FROM src.name
    OR d.company_key IS DISTINCT FROM src.company_key
  );

-- 2) Sada ponovno koristimo CTE za INSERT novih ili izmijenjenih redova
WITH src AS (
  SELECT
    dpt.department_id,
    dpt.name,
    dc.company_key,
    dpt.updated_at
  FROM cleaned.departments AS dpt
  JOIN public.dim_company AS dc
    ON dc.company_id = dpt.company_id
   AND dc.end_date = '9999-12-31'
)
INSERT INTO public.dim_department (
    department_id,
    name,
    company_key,
    start_date,
    end_date
)
SELECT
    src.department_id,
    src.name,
    src.company_key,
    src.updated_at  AS start_date,
    '9999-12-31'::TIMESTAMP AS end_date
FROM src
LEFT JOIN public.dim_department AS d
  ON d.department_id = src.department_id
 AND d.end_date = '9999-12-31'
WHERE d.department_id IS NULL
   OR src.name       IS DISTINCT FROM d.name
   OR src.company_key IS DISTINCT FROM d.company_key;
