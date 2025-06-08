-- 15_populate_fact_device_events.sql

WITH changes AS (
  SELECT
    sp.plug_id,
    sp.status      AS new_status,
    sp.updated_at,
    LAG(sp.status) OVER (
      PARTITION BY sp.plug_id
      ORDER BY sp.updated_at
    ) AS prev_status
  FROM cleaned.smart_plugs AS sp
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
),
agg AS (
  SELECT
    plug_id,
    event_type,
    DATE(updated_at)     AS event_date,
    COUNT(*)             AS event_count,
    MAX(updated_at)      AS created_at
  FROM events
  WHERE event_type IS NOT NULL
  GROUP BY plug_id, event_type, DATE(updated_at)
)
INSERT INTO public.fact_device_events (
  time_id, plug_key, event_type, event_count, created_at
)
SELECT
  dt.time_id,
  dsp.plug_key,
  agg.event_type,
  agg.event_count,
  agg.created_at
FROM agg
JOIN public.dim_time        AS dt
  ON dt.full_date = agg.event_date
JOIN public.dim_smart_plug  AS dsp
  ON dsp.plug_id = agg.plug_id
 AND dsp.end_date = '9999-12-31'
ON CONFLICT (time_id, plug_key, event_type)
DO UPDATE SET
  event_count = EXCLUDED.event_count,
  created_at  = EXCLUDED.created_at;
-- (Optional) Add index for performance:
CREATE INDEX IF NOT EXISTS idx_fact_device_events_time_id
  ON public.fact_device_events(time_id);