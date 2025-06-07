-- 09_scd2_dim_smart_plug.sql

-- 1) UPDATE starih zapisa
WITH src AS (
  SELECT
    c.plug_id,
    du.user_key,
    dr.room_key,
    dd.device_key,
    dc.company_key,
    dp.department_key,
    c.status,
    c.updated_at
  FROM cleaned.smart_plugs AS c
  JOIN public.dim_user AS du
    ON du.user_id = c.user_id
   AND du.end_date = '9999-12-31'
  JOIN public.dim_room AS dr
    ON dr.room_id = c.room_id
   AND dr.end_date = '9999-12-31'
  JOIN public.dim_device AS dd
    ON dd.device_id = c.device_id
   AND dd.end_date = '9999-12-31'
  LEFT JOIN cleaned.company_smart_plugs AS csp
    ON csp.plug_id = c.plug_id
  LEFT JOIN public.dim_company AS dc
    ON dc.company_id = csp.company_id
   AND dc.end_date = '9999-12-31'
  LEFT JOIN cleaned.company_users AS cu
    ON cu.user_id = c.user_id
  LEFT JOIN public.dim_department AS dp
    ON dp.department_id = cu.department_id
   AND dp.end_date = '9999-12-31'
)
UPDATE public.dim_smart_plug AS d
SET end_date = src.updated_at
FROM src
WHERE d.plug_id = src.plug_id
  AND d.end_date = '9999-12-31'
  AND (
       d.user_key       IS DISTINCT FROM src.user_key
    OR d.room_key       IS DISTINCT FROM src.room_key
    OR d.device_key     IS DISTINCT FROM src.device_key
    OR d.company_key    IS DISTINCT FROM src.company_key
    OR d.department_key IS DISTINCT FROM src.department_key
    OR d.status         IS DISTINCT FROM src.status
  );

-- 2) INSERT novih/izmijenjenih zapisa (potrebno opet WITH src)
WITH src AS (
  SELECT
    c.plug_id,
    du.user_key,
    dr.room_key,
    dd.device_key,
    dc.company_key,
    dp.department_key,
    c.status,
    c.updated_at
  FROM cleaned.smart_plugs AS c
  JOIN public.dim_user AS du
    ON du.user_id = c.user_id
   AND du.end_date = '9999-12-31'
  JOIN public.dim_room AS dr
    ON dr.room_id = c.room_id
   AND dr.end_date = '9999-12-31'
  JOIN public.dim_device AS dd
    ON dd.device_id = c.device_id
   AND dd.end_date = '9999-12-31'
  LEFT JOIN cleaned.company_smart_plugs AS csp
    ON csp.plug_id = c.plug_id
  LEFT JOIN public.dim_company AS dc
    ON dc.company_id = csp.company_id
   AND dc.end_date = '9999-12-31'
  LEFT JOIN cleaned.company_users AS cu
    ON cu.user_id = c.user_id
  LEFT JOIN public.dim_department AS dp
    ON dp.department_id = cu.department_id
   AND dp.end_date = '9999-12-31'
)
INSERT INTO public.dim_smart_plug (
    plug_id, user_key, room_key, device_key, company_key, department_key, status, start_date, end_date
)
SELECT
    src.plug_id,
    src.user_key,
    src.room_key,
    src.device_key,
    src.company_key,
    src.department_key,
    src.status,
    src.updated_at AS start_date,
    '9999-12-31'::TIMESTAMP AS end_date
FROM src
LEFT JOIN public.dim_smart_plug AS d
  ON d.plug_id = src.plug_id
 AND d.end_date = '9999-12-31'
WHERE d.plug_id IS NULL
   OR src.user_key       IS DISTINCT FROM d.user_key
   OR src.room_key       IS DISTINCT FROM d.room_key
   OR src.device_key     IS DISTINCT FROM d.device_key
   OR src.company_key    IS DISTINCT FROM d.company_key
   OR src.department_key IS DISTINCT FROM d.department_key
   OR src.status         IS DISTINCT FROM d.status;
