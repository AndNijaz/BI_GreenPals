-- ============================================================
-- SCD2 UPDATE (TYPE 2) ZA SVE ENTITETE u db_analytical
-- Pretpostavke:
--   * landing.* tablice su svježe popunjene (Full Load ili Incremental Load)
--   * archive.* tablice postoje s odgovarajućom strukturom
-- ============================================================

-- ===================================================================
--  A) Archive: users
-- ===================================================================
-- 1) Zatvaramo (end_date = CURRENT_TIMESTAMP) sve aktivne verzije koje
--    su se promijenile u landing.users
UPDATE archive.users AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.users AS l
    WHERE l.id = a.user_id
      AND (
          l.name       IS DISTINCT FROM a.name
       OR l.email      IS DISTINCT FROM a.email
       OR l.eco_points IS DISTINCT FROM a.eco_points
      )
  );

-- 2) Ubacujemo sve nove ili izmijenjene korisnike
INSERT INTO archive.users (
    start_date, end_date, process, source, updated,
    user_id, name, email, eco_points
)
SELECT
    l.updated_at,               -- start_date
    '9999-12-31',               -- end_date
    'scd2_update',              -- process
    'landing.users',            -- source
    l.updated_at,               -- updated
    l.id AS user_id,
    l.name,
    l.email,
    l.eco_points
FROM landing.users AS l
LEFT JOIN archive.users AS a
  ON a.user_id = l.id
 AND a.end_date = '9999-12-31'
WHERE a.user_id IS NULL
   OR l.name       IS DISTINCT FROM a.name
   OR l.email      IS DISTINCT FROM a.email
   OR l.eco_points IS DISTINCT FROM a.eco_points;


-- ===================================================================
--  B) Archive: devices
-- ===================================================================
UPDATE archive.devices AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.devices AS l
    WHERE l.id = a.device_id
      AND (
          l.name     IS DISTINCT FROM a.name
       OR l.category IS DISTINCT FROM a.category
      )
  );

INSERT INTO archive.devices (
    start_date, end_date, process, source, updated,
    device_id, name, category
)
SELECT
    l.updated_at,            -- start_date
    '9999-12-31',            -- end_date
    'scd2_update',           -- process
    'landing.devices',       -- source
    l.updated_at,            -- updated
    l.id     AS device_id,
    l.name,
    l.category
FROM landing.devices AS l
LEFT JOIN archive.devices AS a
  ON a.device_id = l.id
 AND a.end_date = '9999-12-31'
WHERE a.device_id IS NULL
   OR l.name     IS DISTINCT FROM a.name
   OR l.category IS DISTINCT FROM a.category;


-- ===================================================================
--  C) Archive: smart_plugs
-- ===================================================================
UPDATE archive.smart_plugs AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.smart_plugs AS l
    WHERE l.id = a.plug_id
      AND (
          l.user_id   IS DISTINCT FROM a.user_id
       OR l.room_id   IS DISTINCT FROM a.room_id
       OR l.device_id IS DISTINCT FROM a.device_id
       OR l.status    IS DISTINCT FROM a.status
      )
  );

INSERT INTO archive.smart_plugs (
    start_date, end_date, process, source, updated,
    plug_id, user_id, room_id, device_id, status
)
SELECT
    l.updated_at,            -- start_date
    '9999-12-31',            -- end_date
    'scd2_update',           -- process
    'landing.smart_plugs',   -- source
    l.updated_at,            -- updated
    l.id     AS plug_id,
    l.user_id,
    l.room_id,
    l.device_id,
    l.status
FROM landing.smart_plugs AS l
LEFT JOIN archive.smart_plugs AS a
  ON a.plug_id = l.id
 AND a.end_date = '9999-12-31'
WHERE a.plug_id IS NULL
   OR l.user_id   IS DISTINCT FROM a.user_id
   OR l.room_id   IS DISTINCT FROM a.room_id
   OR l.device_id IS DISTINCT FROM a.device_id
   OR l.status    IS DISTINCT FROM a.status;


-- ===================================================================
--  D) Archive: plug_assignments
-- ===================================================================
UPDATE archive.plug_assignments AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.plug_assignments AS l
    WHERE l.id = a.assignment_id
      AND (
          l.plug_id   IS DISTINCT FROM a.plug_id
       OR l.room_id   IS DISTINCT FROM a.room_id
       OR l.device_id IS DISTINCT FROM a.device_id
       OR l.start_time IS DISTINCT FROM a.start_time
       OR l.end_time   IS DISTINCT FROM a.end_time
      )
  );

INSERT INTO archive.plug_assignments (
    start_date, end_date, process, source, updated,
    assignment_id, plug_id, room_id, device_id, start_time, end_time
)
SELECT
    l.updated_at,                  -- start_date
    '9999-12-31',                  -- end_date
    'scd2_update',                 -- process
    'landing.plug_assignments',    -- source
    l.updated_at,                  -- updated
    l.id        AS assignment_id,
    l.plug_id,
    l.room_id,
    l.device_id,
    l.start_time,
    l.end_time
FROM landing.plug_assignments AS l
LEFT JOIN archive.plug_assignments AS a
  ON a.assignment_id = l.id
 AND a.end_date = '9999-12-31'
WHERE a.assignment_id IS NULL
   OR l.plug_id   IS DISTINCT FROM a.plug_id
   OR l.room_id   IS DISTINCT FROM a.room_id
   OR l.device_id IS DISTINCT FROM a.device_id
   OR l.start_time IS DISTINCT FROM a.start_time
   OR l.end_time   IS DISTINCT FROM a.end_time;


-- ===================================================================
--  E) Archive: readings
-- ===================================================================
UPDATE archive.readings AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.readings AS l
    WHERE l.plug_id = a.plug_id
      AND l.timestamp = a.timestamp
      AND (
          l.power_kwh IS DISTINCT FROM a.power_kwh
      )
      AND a.end_date = '9999-12-31'
  );

INSERT INTO archive.readings (
    start_date, end_date, process, source, updated,
    plug_id, timestamp, power_kwh
)
SELECT
    l.updated_at,           -- start_date
    '9999-12-31',           -- end_date
    'scd2_update',          -- process
    'landing.readings',     -- source
    l.updated_at,           -- updated
    l.plug_id,
    l.timestamp,
    l.power_kwh
FROM landing.readings AS l
LEFT JOIN archive.readings AS a
  ON l.plug_id = a.plug_id
 AND l.timestamp = a.timestamp
 AND a.end_date = '9999-12-31'
WHERE a.plug_id IS NULL
   OR a.timestamp IS NULL
   OR l.power_kwh IS DISTINCT FROM a.power_kwh;


-- ===================================================================
--  F) Archive: company_users
-- ===================================================================
UPDATE archive.company_users AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.company_users AS l
    WHERE l.id = a.user_id
      AND (
          l.company_id   IS DISTINCT FROM a.company_id
       OR l.name         IS DISTINCT FROM a.name
       OR l.email        IS DISTINCT FROM a.email
       OR l.department_id IS DISTINCT FROM a.department_id
       OR l.role         IS DISTINCT FROM a.role
      )
  );

INSERT INTO archive.company_users (
    start_date, end_date, process, source, updated,
    user_id, company_id, name, email, department_id, role
)
SELECT
    l.updated_at,               -- start_date
    '9999-12-31',               -- end_date
    'scd2_update',              -- process
    'landing.company_users',    -- source
    l.updated_at,               -- updated
    l.id      AS user_id,
    l.company_id,
    l.name,
    l.email,
    l.department_id,
    l.role
FROM landing.company_users AS l
LEFT JOIN archive.company_users AS a
  ON a.user_id = l.id
 AND a.end_date = '9999-12-31'
WHERE a.user_id IS NULL
   OR l.company_id   IS DISTINCT FROM a.company_id
   OR l.name         IS DISTINCT FROM a.name
   OR l.email        IS DISTINCT FROM a.email
   OR l.department_id IS DISTINCT FROM a.department_id
   OR l.role         IS DISTINCT FROM a.role;


-- ===================================================================
--  G) Archive: company_devices
-- ===================================================================
UPDATE archive.company_devices AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.company_devices AS l
    WHERE l.id = a.device_id
      AND (
          l.name     IS DISTINCT FROM a.name
       OR l.category IS DISTINCT FROM a.category
      )
  );

INSERT INTO archive.company_devices (
    start_date, end_date, process, source, updated,
    device_id, name, category
)
SELECT
    l.updated_at,                -- start_date
    '9999-12-31',                -- end_date
    'scd2_update',               -- process
    'landing.company_devices',   -- source
    l.updated_at,                -- updated
    l.id      AS device_id,
    l.name,
    l.category
FROM landing.company_devices AS l
LEFT JOIN archive.company_devices AS a
  ON a.device_id = l.id
 AND a.end_date = '9999-12-31'
WHERE a.device_id IS NULL
   OR l.name     IS DISTINCT FROM a.name
   OR l.category IS DISTINCT FROM a.category;


-- ===================================================================
--  H) Archive: company_readings
-- ===================================================================
UPDATE archive.company_readings AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.company_readings AS l
    WHERE l.plug_id = a.plug_id
      AND l.timestamp = a.timestamp
      AND (
          l.power_kwh IS DISTINCT FROM a.power_kwh
      )
      AND a.end_date = '9999-12-31'
  );

INSERT INTO archive.company_readings (
    start_date, end_date, process, source, updated,
    plug_id, timestamp, power_kwh
)
SELECT
    l.updated_at,               -- start_date
    '9999-12-31',               -- end_date
    'scd2_update',              -- process
    'landing.company_readings', -- source
    l.updated_at,               -- updated
    l.plug_id,
    l.timestamp,
    l.power_kwh
FROM landing.company_readings AS l
LEFT JOIN archive.company_readings AS a
  ON l.plug_id = a.plug_id
 AND l.timestamp = a.timestamp
 AND a.end_date = '9999-12-31'
WHERE a.plug_id IS NULL
   OR a.timestamp IS NULL
   OR l.power_kwh IS DISTINCT FROM a.power_kwh;


-- ===================================================================
--  I) Archive: company_smart_plugs
-- ===================================================================
UPDATE archive.company_smart_plugs AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.company_smart_plugs AS l
    WHERE l.id = a.plug_id
      AND (
          l.company_id IS DISTINCT FROM a.company_id
       OR l.room_id    IS DISTINCT FROM a.room_id
       OR l.device_id  IS DISTINCT FROM a.device_id
       OR l.status     IS DISTINCT FROM a.status
      )
  );

INSERT INTO archive.company_smart_plugs (
    start_date, end_date, process, source, updated,
    plug_id, company_id, room_id, device_id, status
)
SELECT
    l.updated_at,                 -- start_date
    '9999-12-31',                 -- end_date
    'scd2_update',                -- process
    'landing.company_smart_plugs',-- source
    l.updated_at,                 -- updated
    l.id       AS plug_id,
    l.company_id,
    l.room_id,
    l.device_id,
    l.status
FROM landing.company_smart_plugs AS l
LEFT JOIN archive.company_smart_plugs AS a
  ON a.plug_id = l.id
 AND a.end_date = '9999-12-31'
WHERE a.plug_id IS NULL
   OR l.company_id IS DISTINCT FROM a.company_id
   OR l.room_id    IS DISTINCT FROM a.room_id
   OR l.device_id  IS DISTINCT FROM a.device_id
   OR l.status     IS DISTINCT FROM a.status;


-- ===================================================================
--  J) Archive: companies
-- ===================================================================
UPDATE archive.companies AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.companies AS l
    WHERE l.id = a.company_id
      AND (
          l.name     IS DISTINCT FROM a.name
       OR l.industry IS DISTINCT FROM a.industry
      )
  );

INSERT INTO archive.companies (
    start_date, end_date, process, source, updated,
    company_id, name, industry
)
SELECT
    l.updated_at,           -- start_date
    '9999-12-31',           -- end_date
    'scd2_update',          -- process
    'landing.companies',    -- source
    l.updated_at,           -- updated
    l.id       AS company_id,
    l.name,
    l.industry
FROM landing.companies AS l
LEFT JOIN archive.companies AS a
  ON a.company_id = l.id
 AND a.end_date = '9999-12-31'
WHERE a.company_id IS NULL
   OR l.name     IS DISTINCT FROM a.name
   OR l.industry IS DISTINCT FROM a.industry;


-- ===================================================================
--  K) Archive: locations
-- ===================================================================
UPDATE archive.locations AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.locations AS l
    WHERE l.id = a.location_id
      AND (
          l.name    IS DISTINCT FROM a.name
       OR l.country IS DISTINCT FROM a.country
      )
  );

INSERT INTO archive.locations (
    start_date, end_date, process, source, updated,
    location_id, name, country
)
SELECT
    l.updated_at,             -- start_date
    '9999-12-31',             -- end_date
    'scd2_update',            -- process
    'landing.locations',      -- source
    l.updated_at,             -- updated
    l.id       AS location_id,
    l.name,
    l.country
FROM landing.locations AS l
LEFT JOIN archive.locations AS a
  ON a.location_id = l.id
 AND a.end_date = '9999-12-31'
WHERE a.location_id IS NULL
   OR l.name    IS DISTINCT FROM a.name
   OR l.country IS DISTINCT FROM a.country;


-- ===================================================================
--  L) Archive: rooms
-- ===================================================================
UPDATE archive.rooms AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.rooms AS l
    WHERE l.id = a.room_id
      AND (
          l.name        IS DISTINCT FROM a.name
       OR l.location_id IS DISTINCT FROM a.location_id
      )
  );

INSERT INTO archive.rooms (
    start_date, end_date, process, source, updated,
    room_id, name, location_id
)
SELECT
    l.updated_at,             -- start_date
    '9999-12-31',             -- end_date
    'scd2_update',            -- process
    'landing.rooms',          -- source
    l.updated_at,             -- updated
    l.id       AS room_id,
    l.name,
    l.location_id
FROM landing.rooms AS l
LEFT JOIN archive.rooms AS a
  ON a.room_id = l.id
 AND a.end_date = '9999-12-31'
WHERE a.room_id IS NULL
   OR l.name        IS DISTINCT FROM a.name
   OR l.location_id IS DISTINCT FROM a.location_id;


-- ===================================================================
--  M) Archive: company_locations
-- ===================================================================
UPDATE archive.company_locations AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.company_locations AS l
    WHERE l.id = a.company_location_id
      AND (
          l.name       IS DISTINCT FROM a.name
       OR l.country    IS DISTINCT FROM a.country
       OR l.co2_factor IS DISTINCT FROM a.co2_factor
       OR l.company_id IS DISTINCT FROM a.company_id
      )
  );

INSERT INTO archive.company_locations (
    start_date, end_date, process, source, updated,
    company_location_id, name, country, co2_factor, company_id
)
SELECT
    l.updated_at,                   -- start_date
    '9999-12-31',                   -- end_date
    'scd2_update',                  -- process
    'landing.company_locations',    -- source
    l.updated_at,                   -- updated
    l.id      AS company_location_id,
    l.name,
    l.country,
    l.co2_factor,
    l.company_id
FROM landing.company_locations AS l
LEFT JOIN archive.company_locations AS a
  ON a.company_location_id = l.id
 AND a.end_date = '9999-12-31'
WHERE a.company_location_id IS NULL
   OR l.name       IS DISTINCT FROM a.name
   OR l.country    IS DISTINCT FROM a.country
   OR l.co2_factor IS DISTINCT FROM a.co2_factor
   OR l.company_id IS DISTINCT FROM a.company_id;


-- ===================================================================
--  N) Archive: company_rooms
-- ===================================================================
UPDATE archive.company_rooms AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.company_rooms AS l
    WHERE l.id = a.company_room_id
      AND (
          l.name        IS DISTINCT FROM a.name
       OR l.location_id IS DISTINCT FROM a.location_id
      )
  );

INSERT INTO archive.company_rooms (
    start_date, end_date, process, source, updated,
    company_room_id, name, location_id
)
SELECT
    l.updated_at,                   -- start_date
    '9999-12-31',                   -- end_date
    'scd2_update',                  -- process
    'landing.company_rooms',        -- source
    l.updated_at,                   -- updated
    l.id             AS company_room_id,
    l.name,
    l.location_id
FROM landing.company_rooms AS l
LEFT JOIN archive.company_rooms AS a
  ON a.company_room_id = l.id
 AND a.end_date = '9999-12-31'
WHERE a.company_room_id IS NULL
   OR l.name        IS DISTINCT FROM a.name
   OR l.location_id IS DISTINCT FROM a.location_id;


-- ===================================================================
--  O) Archive: departments
-- ===================================================================
UPDATE archive.departments AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.departments AS l
    WHERE l.id = a.department_id
      AND (
          l.company_id IS DISTINCT FROM a.company_id
       OR l.name       IS DISTINCT FROM a.name
      )
  );

INSERT INTO archive.departments (
    start_date, end_date, process, source, updated,
    department_id, company_id, name
)
SELECT
    l.updated_at,                    -- start_date
    '9999-12-31',                    -- end_date
    'scd2_update',                   -- process
    'landing.departments',           -- source
    l.updated_at,                    -- updated
    l.id        AS department_id,
    l.company_id,
    l.name
FROM landing.departments AS l
LEFT JOIN archive.departments AS a
  ON a.department_id = l.id
 AND a.end_date = '9999-12-31'
WHERE a.department_id IS NULL
   OR l.company_id IS DISTINCT FROM a.company_id
   OR l.name       IS DISTINCT FROM a.name;


-- ===================================================================
--  P) Archive: company_plug_assignments
-- ===================================================================
UPDATE archive.company_plug_assignments AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.company_plug_assignments AS l
    WHERE l.id = a.company_plug_assignment_id
      AND (
          l.plug_id   IS DISTINCT FROM a.plug_id
       OR l.room_id   IS DISTINCT FROM a.room_id
       OR l.device_id IS DISTINCT FROM a.device_id
       OR l.start_time IS DISTINCT FROM a.start_time
       OR l.end_time   IS DISTINCT FROM a.end_time
      )
  );

INSERT INTO archive.company_plug_assignments (
    start_date, end_date, process, source, updated,
    company_plug_assignment_id, plug_id, room_id, device_id, start_time, end_time
)
SELECT
    l.updated_at,                         -- start_date
    '9999-12-31',                         -- end_date
    'scd2_update',                        -- process
    'landing.company_plug_assignments',   -- source
    l.updated_at,                         -- updated
    l.id        AS company_plug_assignment_id,
    l.plug_id,
    l.room_id,
    l.device_id,
    l.start_time,
    l.end_time
FROM landing.company_plug_assignments AS l
LEFT JOIN archive.company_plug_assignments AS a
  ON a.company_plug_assignment_id = l.id
 AND a.end_date = '9999-12-31'
WHERE a.company_plug_assignment_id IS NULL
   OR l.plug_id   IS DISTINCT FROM a.plug_id
   OR l.room_id   IS DISTINCT FROM a.room_id
   OR l.device_id IS DISTINCT FROM a.device_id
   OR l.start_time IS DISTINCT FROM a.start_time
   OR l.end_time   IS DISTINCT FROM a.end_time;


-- ===================================================================
--  Q) Archive: company_smart_plugs (već pokriveno gore u točki I)
--      (OSTAVLJENO RADI STRUKTURE, premda je označeno ranije)
--    (U ovoj skripti ne dupliciramo točku I, već je to integrirano)
-- ===================================================================
-- (ovo je ista logika kao tabaht I gore, preskočeno da ne ponavljamo)


-- ===================================================================
--  ZADNJI KORAK: ako trebaš još koju entitetu, ponovi isti uzorak
-- ===================================================================

-- ================================================================
--   Kraj SCD2 logike. 
-- Sada su landing tablice osvježene, a archive tablice ažurirane
-- da sadrže povijest promjena.
-- ================================================================

