-- 14_create_fact_device_events.sql
CREATE TABLE IF NOT EXISTS public.fact_device_events (
  time_id      INT        NOT NULL,
  plug_key     INT        NOT NULL,
  event_type   TEXT       NOT NULL,   -- 'ON' ili 'OFF'
  event_count  INT        NOT NULL,
  created_at   TIMESTAMP,
  PRIMARY KEY (time_id, plug_key, event_type)
);
-- (Optional) Add index for performance:
CREATE INDEX IF NOT EXISTS idx_fact_device_events_time_id
  ON public.fact_device_events(time_id);