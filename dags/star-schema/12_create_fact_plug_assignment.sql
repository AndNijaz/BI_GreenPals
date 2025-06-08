-- 12_create_fact_plug_assignment.sql
CREATE TABLE IF NOT EXISTS public.fact_plug_assignment (
  time_id        INT        NOT NULL,
  plug_key       INT        NOT NULL,
  device_key     INT        NOT NULL,
  location_key   INT        NOT NULL,
  assignment_cnt INT        NOT NULL,
  usage_hours    DECIMAL(8,2) NOT NULL,
  created_at     TIMESTAMP,
  PRIMARY KEY (time_id, plug_key)
);
-- (Optional) Add index for performance:
CREATE INDEX IF NOT EXISTS idx_fact_plug_assignment_time_id
  ON public.fact_plug_assignment(time_id);