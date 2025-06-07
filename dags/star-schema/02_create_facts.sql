-- 02_create_facts.sql
-- Kreira tablicu fact_readings u public shemi

CREATE TABLE IF NOT EXISTS public.fact_readings (
  time_id    BIGINT       NOT NULL,  -- foreign key na dim_time.time_id
  plug_key   BIGINT       NOT NULL,  -- foreign key na dim_smart_plug.plug_key
  power_kwh  DECIMAL(6,3) NOT NULL,
  created_at TIMESTAMP    NOT NULL,
  PRIMARY KEY (time_id, plug_key)
);

-- (Opcionalno) Dodaj index radi performansi:
CREATE INDEX IF NOT EXISTS idx_fact_readings_time_id
  ON public.fact_readings(time_id);

CREATE INDEX IF NOT EXISTS idx_fact_readings_plug_key
  ON public.fact_readings(plug_key);
