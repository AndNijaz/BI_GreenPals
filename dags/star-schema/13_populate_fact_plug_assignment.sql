-- 13_populate_fact_plug_assignment.sql

WITH assigned AS (
  SELECT
    pa.plug_id,
    pa.device_id,
    pa.room_id,
    pa.start_time,
    COALESCE(pa.end_time, CURRENT_TIMESTAMP) AS end_time,
    pa.updated_at
  FROM cleaned.plug_assignments AS pa
),
agg AS (
  SELECT
    plug_id,
    device_id,
    room_id,
    DATE(start_time)         AS assign_date,
    COUNT(*)                 AS assignment_cnt,
    SUM(EXTRACT(EPOCH FROM (end_time - start_time)) / 3600) AS usage_hours,
    MAX(updated_at)          AS created_at
  FROM assigned
  GROUP BY plug_id, device_id, room_id, DATE(start_time)
)
INSERT INTO public.fact_plug_assignment (
  time_id, plug_key, device_key, location_key,
  assignment_cnt, usage_hours, created_at
)
SELECT
  dt.time_id,
  dsp.plug_key,
  dd.device_key,
  dl.location_key,
  agg.assignment_cnt,
  agg.usage_hours,
  agg.created_at
FROM agg
JOIN public.dim_time         AS dt
  ON dt.full_date = agg.assign_date
JOIN public.dim_smart_plug   AS dsp
  ON dsp.plug_id = agg.plug_id
 AND dsp.end_date = '9999-12-31'
JOIN public.dim_device       AS dd
  ON dd.device_id = agg.device_id
 AND dd.end_date = '9999-12-31'
JOIN public.dim_location     AS dl
  ON dl.location_id = agg.room_id
 AND dl.end_date = '9999-12-31'
ON CONFLICT (time_id, plug_key)
DO UPDATE SET
  assignment_cnt = EXCLUDED.assignment_cnt,
  usage_hours    = EXCLUDED.usage_hours,
  created_at     = EXCLUDED.created_at;
-- (Optional) Add index for performance:
CREATE INDEX IF NOT EXISTS idx_fact_plug_assignment_time_id
  ON public.fact_plug_assignment(time_id);