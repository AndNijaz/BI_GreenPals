/* =======================================================================
   Incremental Star-Schema Load  – prilagođeno tvojim tablicama
   – dim_*   : Type-2 (start_date / end_date, bez is_current)
   – fact_*  : append / upsert (inkrementalno)
======================================================================= */

/*-------------------------------------------------------------
  0) BOOTSTRAP  – sekvence (sigurno za ponovni rad)
-------------------------------------------------------------*/
DO $$
BEGIN
  -- kreiraj sekvence za dimenzije ako ne postoje
  CREATE SEQUENCE IF NOT EXISTS seq_user_key;
  CREATE SEQUENCE IF NOT EXISTS seq_location_key;
  CREATE SEQUENCE IF NOT EXISTS seq_room_key;
  CREATE SEQUENCE IF NOT EXISTS seq_device_key;
  CREATE SEQUENCE IF NOT EXISTS seq_company_key;
  CREATE SEQUENCE IF NOT EXISTS seq_department_key;
  CREATE SEQUENCE IF NOT EXISTS seq_plug_key;

  -- poravnaj svaku sekvencu na max ključ iz postojeće dimenzije
  PERFORM setval('seq_user_key',       COALESCE((SELECT MAX(user_key)       FROM dim_user),       0));
  PERFORM setval('seq_location_key',   COALESCE((SELECT MAX(location_key)   FROM dim_location),   0));
  PERFORM setval('seq_room_key',       COALESCE((SELECT MAX(room_key)       FROM dim_room),       0));
  PERFORM setval('seq_device_key',     COALESCE((SELECT MAX(device_key)     FROM dim_device),     0));
  PERFORM setval('seq_company_key',    COALESCE((SELECT MAX(company_key)    FROM dim_company),    0));
  PERFORM setval('seq_department_key', COALESCE((SELECT MAX(department_key) FROM dim_department), 0));
  PERFORM setval('seq_plug_key',       COALESCE((SELECT MAX(plug_key)       FROM dim_smart_plug), 0));
END
$$;

BEGIN;

/*-----------------------------------------------------------------------
  1) DIMENZIJE  (Type-2 merge, aktivna verzija = end_date = '9999-12-31')
----------------------------------------------------------------------*/

/****************  DIM_USER  *******************************************/
UPDATE dim_user d
SET    end_date = a.start_date - INTERVAL '1 sec'
FROM   archive.users a
WHERE  d.user_id  = a.user_id
  AND  d.end_date = '9999-12-31'
  AND  a.end_date = '9999-12-31'
  AND (a.name       IS DISTINCT FROM d.name
    OR a.email      IS DISTINCT FROM d.email
    OR a.eco_points IS DISTINCT FROM d.eco_points);

INSERT INTO dim_user (user_key, user_id, name, email, eco_points,
                      start_date, end_date)
SELECT nextval('seq_user_key'), a.user_id, a.name, a.email, a.eco_points,
       a.start_date, a.end_date
FROM   archive.users a
LEFT  JOIN dim_user d
       ON d.user_id = a.user_id AND d.start_date = a.start_date
WHERE  d.user_id IS NULL;

/****************  DIM_LOCATION  ***************************************/
UPDATE dim_location d
SET    end_date = a.start_date - INTERVAL '1 sec'
FROM   archive.locations a
WHERE  d.location_id = a.location_id
  AND  d.end_date    = '9999-12-31'
  AND  a.end_date    = '9999-12-31'
  AND (a.name IS DISTINCT FROM d.name
    OR a.country IS DISTINCT FROM d.country);

INSERT INTO dim_location (location_key, location_id, name, country,
                          start_date, end_date)
SELECT nextval('seq_location_key'), a.location_id, a.name, a.country,
       a.start_date, a.end_date
FROM   archive.locations a
LEFT  JOIN dim_location d
       ON d.location_id = a.location_id AND d.start_date = a.start_date
WHERE  d.location_id IS NULL;

/****************  DIM_ROOM  *******************************************/
UPDATE dim_room d
SET    end_date     = a.start_date - INTERVAL '1 sec',
       location_key = dl.location_key
FROM   archive.rooms a
JOIN   dim_location dl
       ON dl.location_id = a.location_id AND dl.end_date = '9999-12-31'
WHERE  d.room_id  = a.room_id
  AND  d.end_date = '9999-12-31'
  AND (a.name IS DISTINCT FROM d.name
    OR dl.location_key IS DISTINCT FROM d.location_key);

INSERT INTO dim_room (room_key, room_id, name, location_key,
                      start_date, end_date)
SELECT nextval('seq_room_key'), a.room_id, a.name, dl.location_key,
       a.start_date, a.end_date
FROM   archive.rooms a
JOIN   dim_location dl
       ON dl.location_id = a.location_id AND dl.end_date = '9999-12-31'
LEFT  JOIN dim_room d
       ON d.room_id = a.room_id AND d.start_date = a.start_date
WHERE  d.room_id IS NULL;

/****************  DIM_DEVICE  *****************************************/
UPDATE dim_device d
SET    end_date = a.start_date - INTERVAL '1 sec'
FROM   archive.devices a
WHERE  d.device_id = a.device_id
  AND  d.end_date  = '9999-12-31'
  AND  a.end_date  = '9999-12-31'
  AND (a.name IS DISTINCT FROM d.name
    OR a.category IS DISTINCT FROM d.category);

INSERT INTO dim_device (device_key, device_id, name, category,
                        start_date, end_date)
SELECT nextval('seq_device_key'), a.device_id, a.name, a.category,
       a.start_date, a.end_date
FROM   archive.devices a
LEFT  JOIN dim_device d
       ON d.device_id = a.device_id AND d.start_date = a.start_date
WHERE  d.device_id IS NULL;

/****************  DIM_COMPANY  ***************************************/
UPDATE dim_company d
SET    end_date = a.start_date - INTERVAL '1 sec'
FROM   archive.companies a
WHERE  d.company_id = a.company_id
  AND  d.end_date   = '9999-12-31'
  AND  a.end_date   = '9999-12-31'
  AND (a.name IS DISTINCT FROM d.name
    OR a.industry IS DISTINCT FROM d.industry);

INSERT INTO dim_company (company_key, company_id, name, industry,
                         start_date, end_date)
SELECT nextval('seq_company_key'), a.company_id, a.name, a.industry,
       a.start_date, a.end_date
FROM   archive.companies a
LEFT  JOIN dim_company d
       ON d.company_id = a.company_id AND d.start_date = a.start_date
WHERE  d.company_id IS NULL;

/****************  DIM_DEPARTMENT  *************************************/
UPDATE dim_department d
SET    end_date    = a.start_date - INTERVAL '1 sec',
       company_key = dc.company_key
FROM   archive.departments a
JOIN   dim_company dc
       ON dc.company_id = a.company_id AND dc.end_date = '9999-12-31'
WHERE  d.department_id = a.department_id
  AND  d.end_date      = '9999-12-31'
  AND (a.name IS DISTINCT FROM d.name
    OR dc.company_key IS DISTINCT FROM d.company_key);

INSERT INTO dim_department (department_key, department_id, company_key, name,
                            start_date, end_date)
SELECT nextval('seq_department_key'), a.department_id, dc.company_key, a.name,
       a.start_date, a.end_date
FROM   archive.departments a
JOIN   dim_company dc
       ON dc.company_id = a.company_id AND dc.end_date = '9999-12-31'
LEFT  JOIN dim_department d
       ON d.department_id = a.department_id AND d.start_date = a.start_date
WHERE  d.department_id IS NULL;

/****************  DIM_SMART_PLUG  *************************************/
UPDATE dim_smart_plug d
SET    end_date   = a.start_date - INTERVAL '1 sec',
       user_key   = du.user_key,
       room_key   = dr.room_key,
       device_key = dd.device_key
FROM   archive.smart_plugs a
JOIN   dim_user   du ON du.user_id   = a.user_id   AND du.end_date = '9999-12-31'
JOIN   dim_room   dr ON dr.room_id   = a.room_id   AND dr.end_date = '9999-12-31'
JOIN   dim_device dd ON dd.device_id = a.device_id AND dd.end_date = '9999-12-31'
WHERE  d.plug_id  = a.plug_id
  AND  d.end_date = '9999-12-31'
  AND (du.user_key   IS DISTINCT FROM d.user_key
    OR dr.room_key   IS DISTINCT FROM d.room_key
    OR dd.device_key IS DISTINCT FROM d.device_key
    OR a.status      IS DISTINCT FROM d.status);

INSERT INTO dim_smart_plug (plug_key, plug_id, user_key, room_key, device_key,
                            status, start_date, end_date)
SELECT nextval('seq_plug_key'), a.plug_id,
       du.user_key, dr.room_key, dd.device_key,
       a.status, a.start_date, a.end_date
FROM   archive.smart_plugs a
JOIN   dim_user   du ON du.user_id   = a.user_id   AND du.end_date = '9999-12-31'
JOIN   dim_room   dr ON dr.room_id   = a.room_id   AND dr.end_date = '9999-12-31'
JOIN   dim_device dd ON dd.device_id = a.device_id AND dd.end_date = '9999-12-31'
LEFT  JOIN dim_smart_plug d
       ON d.plug_id = a.plug_id AND d.start_date = a.start_date
WHERE  d.plug_id IS NULL;

/*-----------------------------------------------------------------------
  2) FACT TABELE  (inkremental – append / upsert)
----------------------------------------------------------------------*/

/****************  FACT_READINGS  **************************************/
WITH max_ts AS (
    SELECT COALESCE(MAX(created_at),'2000-01-01') AS last_ts
    FROM   fact_readings
)
INSERT INTO fact_readings (time_id, plug_key, power_kwh, created_at)
SELECT dt.time_id,
       dsp.plug_key,
       l.power_kwh,
       l.updated_at
FROM   landing.readings l
JOIN   dim_smart_plug dsp
       ON dsp.plug_id = l.plug_id AND dsp.end_date = '9999-12-31'
JOIN   dim_time dt
       ON dt.full_date = l.timestamp::date
      AND dt.hour      = EXTRACT(HOUR   FROM l.timestamp)::INT
      AND dt.minute    = EXTRACT(MINUTE FROM l.timestamp)::INT
CROSS  JOIN max_ts
WHERE  l.updated_at > max_ts.last_ts
ON CONFLICT (time_id, plug_key)
DO UPDATE SET
      power_kwh  = EXCLUDED.power_kwh,
      created_at = EXCLUDED.created_at;

/****************  FACT_COMPANY_READINGS  ******************************/
WITH 
  max_ts AS (
    SELECT COALESCE(MAX(created_at), '2000-01-01') AS last_ts
      FROM fact_company_readings
  ),
  src AS (
    -- agregiramo po company na minute, uz JOIN na company_smart_plugs
    SELECT
      DATE_TRUNC('minute', l.timestamp) AS minute_ts,
      csp.company_id,
      SUM(l.power_kwh)                 AS power_kwh,
      MAX(l.updated_at)                AS created_at
    FROM landing.company_readings AS l
    JOIN cleaned.company_smart_plugs AS csp
      ON csp.plug_id = l.plug_id
    WHERE l.updated_at > (SELECT last_ts FROM max_ts)
    GROUP BY 1, 2
  )
INSERT INTO fact_company_readings (
    time_id,
    company_key,
    power_kwh,
    created_at
)
SELECT
  dt.time_id,
  dc.company_key,
  s.power_kwh,
  s.created_at
FROM src AS s
JOIN dim_company AS dc
  ON dc.company_id = s.company_id
 AND dc.end_date   = '9999-12-31'
JOIN dim_time AS dt
  ON dt.full_date = s.minute_ts::date
 AND dt.hour      = EXTRACT(HOUR   FROM s.minute_ts)::INT
 AND dt.minute    = EXTRACT(MINUTE FROM s.minute_ts)::INT
ON CONFLICT (time_id, company_key)
DO UPDATE
  SET power_kwh  = EXCLUDED.power_kwh,
      created_at = EXCLUDED.created_at;

/****************  FACT_PLUG_ASSIGNMENT  *******************************/
WITH agg AS (
    SELECT
        pa.plug_id,
        pa.device_id,
        pa.room_id,
        DATE(pa.start_time)                                           AS assign_date,
        COUNT(*)                                                      AS assignment_cnt,
        SUM(
            EXTRACT(EPOCH FROM (COALESCE(pa.end_time, CURRENT_TIMESTAMP)
                                - pa.start_time)) / 3600
        )::NUMERIC(8,2)                                               AS usage_hours,
        MAX(pa.updated_at)                                            AS created_at
    FROM landing.plug_assignments pa
    GROUP BY pa.plug_id, pa.device_id, pa.room_id, DATE(pa.start_time)
), max_dt AS (
    SELECT COALESCE(MAX(created_at),'2000-01-01') AS last_ts
    FROM   fact_plug_assignment
)
INSERT INTO fact_plug_assignment (time_id, plug_key, device_key, location_key,
                                  assignment_cnt, usage_hours, created_at)
SELECT dt.time_id,
       dsp.plug_key,
       dd.device_key,
       dl.location_key,
       agg.assignment_cnt,
       agg.usage_hours,
       agg.created_at
FROM   agg
JOIN   dim_time dt            ON dt.full_date = agg.assign_date
JOIN   dim_smart_plug dsp     ON dsp.plug_id   = agg.plug_id   AND dsp.end_date='9999-12-31'
JOIN   dim_device dd          ON dd.device_id  = agg.device_id AND dd.end_date='9999-12-31'
JOIN   dim_location dl        ON dl.location_id = agg.room_id  AND dl.end_date='9999-12-31'
CROSS  JOIN max_dt
WHERE  agg.created_at > max_dt.last_ts
ON CONFLICT (time_id, plug_key)
DO UPDATE SET
      assignment_cnt = EXCLUDED.assignment_cnt,
      usage_hours    = EXCLUDED.usage_hours,
      created_at     = EXCLUDED.created_at;

/****************  FACT_DEVICE_EVENTS  *********************************/
WITH
  max_ts AS (
    SELECT COALESCE(MAX(created_at), '2000-01-01') AS last_ts
      FROM fact_device_events
  ),
  events AS (
    -- detektiramo ON/OFF iz archive.smart_plugs
    SELECT 
      sp.plug_id,
      CASE
        WHEN LAG(sp.status) OVER (PARTITION BY sp.plug_id ORDER BY sp.updated) = FALSE
             AND sp.status = TRUE  THEN 'ON'
        WHEN LAG(sp.status) OVER (PARTITION BY sp.plug_id ORDER BY sp.updated) = TRUE
             AND sp.status = FALSE THEN 'OFF'
        ELSE NULL
      END AS event_type,
      sp.updated AS updated_at
    FROM archive.smart_plugs AS sp
  ),
  filtered AS (
    -- samo nove evente nakon zadnjeg učitanog created_at
    SELECT *
      FROM events
     WHERE event_type IS NOT NULL
       AND updated_at > (SELECT last_ts FROM max_ts)
  ),
  agg AS (
    -- agregiramo po plug_id + event_type
    SELECT
      plug_id,
      event_type,
      COUNT(*)           AS event_count,
      MAX(updated_at)    AS created_at
    FROM filtered
    GROUP BY plug_id, event_type
  )
INSERT INTO fact_device_events (
    time_id,
    plug_key,
    event_type,
    event_count,
    created_at
)
SELECT
  dt.time_id,
  dsp.plug_key,
  a.event_type,
  a.event_count,
  a.created_at
FROM agg AS a
JOIN dim_smart_plug AS dsp
  ON dsp.plug_id = a.plug_id
 AND dsp.end_date = '9999-12-31'
JOIN dim_time AS dt
  ON dt.full_date = a.created_at::date
 AND dt.hour      = EXTRACT(HOUR   FROM a.created_at)::INT
 AND dt.minute    = EXTRACT(MINUTE FROM a.created_at)::INT
ON CONFLICT (time_id, plug_key, event_type)
DO UPDATE
  SET event_count = EXCLUDED.event_count,
      created_at  = EXCLUDED.created_at;

COMMIT;
