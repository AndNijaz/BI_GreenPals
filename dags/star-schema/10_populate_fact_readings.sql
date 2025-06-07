-- 10_populate_fact_readings.sql
-- Punimo fact_readings iz cleaned.readings, ali grupiramo mjerenja po minuti 
-- kako ne bi bilo duplikata po (time_id, plug_key).

-- 1) U CTE ranked označavamo svaku grupu (plug_id, minute_truncated) s rednim brojem
WITH ranked AS (
  SELECT
    cr.plug_id,
    date_trunc('minute', cr."timestamp") AS ts_minute,  -- truncate timestamp na minutu
    cr.power_kwh,
    cr.updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY cr.plug_id, date_trunc('minute', cr."timestamp")
      ORDER BY cr.updated_at DESC
    ) AS rn
  FROM cleaned.readings AS cr
),

-- 2) U deduped zadržavamo samo one retke s rn = 1 (najnoviji u toj minuti za taj plug_id)
deduped AS (
  SELECT
    plug_id,
    ts_minute,
    power_kwh,
    updated_at
  FROM ranked
  WHERE rn = 1
)

-- 3) Sada ubacujemo u fact_readings, spajajući na dim_time i dim_smart_plug
INSERT INTO public.fact_readings (
  time_id,
  plug_key,
  power_kwh,
  created_at
)
SELECT
  dt.time_id,
  dsp.plug_key,
  d.power_kwh,
  d.updated_at AS created_at
FROM deduped AS d

-- JOIN na dim_time po trunicat–minutari: 
-- dim_time.full_date = DATE(ts_minute), 
-- dim_time.hour      = HOUR(ts_minute), 
-- dim_time.minute    = MINUTE(ts_minute)
JOIN public.dim_time AS dt
  ON dt.full_date = d.ts_minute::DATE
 AND dt.hour      = EXTRACT(HOUR FROM d.ts_minute)::INTEGER
 AND dt.minute    = EXTRACT(MINUTE FROM d.ts_minute)::INTEGER

-- JOIN na dim_smart_plug po plug_id
JOIN public.dim_smart_plug AS dsp
  ON dsp.plug_id = d.plug_id
 AND dsp.end_date = '9999-12-31'

-- Upsert logika:  
ON CONFLICT (time_id, plug_key)
DO UPDATE 
  SET power_kwh  = EXCLUDED.power_kwh,
      created_at = EXCLUDED.created_at;
