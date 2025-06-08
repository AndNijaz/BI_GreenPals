-- 11_populate_fact_company_readings.sql

WITH dedup AS (
  SELECT DISTINCT ON (cr.plug_id, cr."timestamp")
    cr.plug_id,
    cr."timestamp",
    cr.power_kwh,
    cr.updated_at,  -- ✅ ispravka ovdje
    cu.company_id
  FROM cleaned.company_readings AS cr
  JOIN cleaned.company_smart_plugs AS csp
    ON csp.plug_id = cr.plug_id
  JOIN cleaned.company_users AS cu
    ON cu.user_id = cr.plug_id
  ORDER BY cr.plug_id, cr."timestamp", cr.updated_at DESC
),
agg AS (
  SELECT
    date_trunc('minute', d."timestamp")  AS minute_ts,
    d.company_id,
    SUM(d.power_kwh)                    AS total_power,
    MAX(d.updated_at)                   AS max_updated  -- ✅ ovdje također
  FROM dedup AS d
  GROUP BY 1, 2
)
INSERT INTO public.fact_company_readings (
  time_id, company_key, power_kwh, created_at
)
SELECT
  dt.time_id,
  dc.company_key,
  a.total_power,
  a.max_updated
FROM agg AS a
JOIN public.dim_time AS dt
  ON dt.full_date = a.minute_ts::date
 AND dt.hour      = EXTRACT(HOUR FROM a.minute_ts)::int
 AND dt.minute    = EXTRACT(MINUTE FROM a.minute_ts)::int
JOIN public.dim_company AS dc
  ON dc.company_id = a.company_id
 AND dc.end_date = '9999-12-31'
ON CONFLICT (time_id, company_key)
DO UPDATE
  SET power_kwh  = EXCLUDED.power_kwh,
      created_at = EXCLUDED.created_at;
