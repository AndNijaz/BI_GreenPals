WITH dedup AS (
  SELECT DISTINCT ON (plug_id, "timestamp")
    cr.plug_id,
    cr."timestamp",
    cr.power_kwh,
    cr.updated_at,
    cu.company_id
  FROM cleaned.company_readings AS cr
  JOIN cleaned.company_users   AS cu ON cu.user_id = cr.plug_id  -- ili kako se spajaš na company context
  ORDER BY plug_id, "timestamp", updated_at DESC
)
INSERT INTO public.fact_company_readings (time_id, company_key, power_kwh, created_at)
SELECT
  dt.time_id,
  dc.company_key,
  d.power_kwh,
  d.updated_at
FROM dedup AS d
JOIN public.dim_time    AS dt ON dt.full_date = d."timestamp"::date
                           AND dt.hour      = EXTRACT(HOUR   FROM d."timestamp")
                           AND dt.minute    = EXTRACT(MINUTE FROM d."timestamp")
JOIN public.dim_company AS dc ON dc.company_id = d.company_id
                           AND dc.end_date = '9999-12-31'
ON CONFLICT (time_id, company_key)
DO UPDATE SET
  power_kwh  = EXCLUDED.power_kwh,
  created_at = EXCLUDED.created_at;
