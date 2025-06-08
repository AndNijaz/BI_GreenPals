-- 15_populate_fact_device_events.sql
-- ETL za fact_device_events: detektiramo ON/OFF evente iz archive.smart_plugs
-- i ubacujemo ih (s agregacijom po minuti) u public.fact_device_events.

WITH changes AS (
  SELECT
    sp.plug_id,
    sp.status      AS new_status,
    sp.updated     AS updated_at,  -- ✅ ispravljeno ovdje
    LAG(sp.status) OVER (
      PARTITION BY sp.plug_id
      ORDER BY sp.updated          -- ✅ i ovdje
    ) AS prev_status
  FROM archive.smart_plugs AS sp
),
events AS (
  SELECT
    plug_id,
    CASE
      WHEN prev_status = FALSE AND new_status = TRUE THEN 'ON'
      WHEN prev_status = TRUE  AND new_status = FALSE THEN 'OFF'
      ELSE NULL
    END AS event_type,
    updated_at
  FROM changes
)

INSERT INTO public.fact_device_events (
  time_id,
  plug_key,
  event_type,
  event_count,
  created_at
)
SELECT
  dt.time_id,
  dsp.plug_key,
  e.event_type,
  COUNT(*)           AS event_count,
  MAX(e.updated_at)  AS created_at
FROM events AS e

-- spajanje na dim_time po minuti
JOIN public.dim_time AS dt
  ON dt.full_date = e.updated_at::date
 AND dt.hour      = EXTRACT(HOUR   FROM e.updated_at)::INT
 AND dt.minute    = EXTRACT(MINUTE FROM e.updated_at)::INT

-- spajanje na aktivni smart_plug u dimenziji
JOIN public.dim_smart_plug AS dsp
  ON dsp.plug_id = e.plug_id
 AND dsp.end_date = '9999-12-31'

WHERE e.event_type IS NOT NULL

GROUP BY dt.time_id, dsp.plug_key, e.event_type

ON CONFLICT (time_id, plug_key, event_type)
DO UPDATE
  SET event_count = EXCLUDED.event_count,
      created_at  = EXCLUDED.created_at;
