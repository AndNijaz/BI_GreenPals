CREATE TABLE IF NOT EXISTS public.fact_company_readings (
  time_id      INT       NOT NULL,
  company_key  INT       NOT NULL,
  power_kwh    DECIMAL(6,3),
  created_at   TIMESTAMP,
  PRIMARY KEY (time_id, company_key)
);
-- (Optional) Add index for performance:
CREATE INDEX IF NOT EXISTS idx_fact_company_readings_time_id
  ON public.fact_company_readings(time_id);