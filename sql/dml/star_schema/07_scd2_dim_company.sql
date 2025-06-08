-- 07_scd2_dim_company.sql
-- SCD2 update za dim_company – puni iz cleaned.companies

-- 1) Zatvorimo stare verzije
UPDATE public.dim_company AS d
SET end_date = c.updated_at
FROM cleaned.companies AS c
WHERE d.company_id = c.company_id
  AND d.end_date = '9999-12-31'
  AND (
       d.name     IS DISTINCT FROM c.name
    OR d.industry IS DISTINCT FROM c.industry
  );

-- 2) Ubacimo nove verzije
INSERT INTO public.dim_company (
    company_id, name, industry, start_date, end_date
)
SELECT
    c.company_id,
    c.name,
    c.industry,
    c.updated_at AS start_date,
    '9999-12-31'::TIMESTAMP AS end_date
FROM cleaned.companies AS c
LEFT JOIN public.dim_company AS d
  ON d.company_id = c.company_id
 AND d.end_date = '9999-12-31'
WHERE d.company_id IS NULL
   OR c.name     IS DISTINCT FROM d.name
   OR c.industry IS DISTINCT FROM d.industry;
