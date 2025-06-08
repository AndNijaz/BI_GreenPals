-- ============================================================
-- scd2_cleaned.sql
--   SCD2 UPDATE za sve entitete iz cleaned sheme u archive_cleaned
-- Pretpostavke:
--   * cleaned.* tablice sadržavaju “aktivan” snapshot (end_date = ’9999-12-31’)
--   * archive_cleaned.* tablice postoje s istom strukturom kakvu imaju archive.*,
--     ali za cleaned podatke (npr. archive_cleaned.users, archive_cleaned.locations, itd.)
-- ============================================================

-- ===================================================================
--  A) Archive_cleaned: users
-- ===================================================================
-- 1) Zatvaramo stare verzije (end_date = CURRENT_TIMESTAMP) ako se podaci u cleaned.users promijenili
UPDATE archive_cleaned.users AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM cleaned.users AS c
    WHERE c.user_id = a.user_id
      AND (
          c.name       IS DISTINCT FROM a.name
       OR c.email      IS DISTINCT FROM a.email
       OR c.eco_points IS DISTINCT FROM a.eco_points
      )
  );

-- 2) Ubacujemo nove ili izmijenjene retke iz cleaned.users
INSERT INTO archive_cleaned.users (
    start_date, end_date, process, source, updated,
    user_id, name, email, eco_points
)
SELECT
    c.updated_at,               -- start_date
    '9999-12-31',               -- end_date
    'scd2_cleaned',             -- process
    'cleaned.users',            -- source
    c.updated_at,               -- updated
    c.user_id,
    c.name,
    c.email,
    c.eco_points
FROM cleaned.users AS c
LEFT JOIN archive_cleaned.users AS a
  ON a.user_id = c.user_id
 AND a.end_date = '9999-12-31'
WHERE a.user_id IS NULL
   OR c.name       IS DISTINCT FROM a.name
   OR c.email      IS DISTINCT FROM a.email
   OR c.eco_points IS DISTINCT FROM a.eco_points;


-- ===================================================================
--  B) Archive_cleaned: locations
-- ===================================================================
UPDATE archive_cleaned.locations AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM cleaned.locations AS c
    WHERE c.location_id = a.location_id
      AND (
          c.name    IS DISTINCT FROM a.name
       OR c.country IS DISTINCT FROM a.country
      )
  );

INSERT INTO archive_cleaned.locations (
    start_date, end_date, process, source, updated,
    location_id, name, country
)
SELECT
    c.updated_at,             -- start_date
    '9999-12-31',             -- end_date
    'scd2_cleaned',           -- process
    'cleaned.locations',      -- source
    c.updated_at,             -- updated
    c.location_id,
    c.name,
    c.country
FROM cleaned.locations AS c
LEFT JOIN archive_cleaned.locations AS a
  ON a.location_id = c.location_id
 AND a.end_date = '9999-12-31'
WHERE a.location_id IS NULL
   OR c.name    IS DISTINCT FROM a.name
   OR c.country IS DISTINCT FROM a.country;


-- ===================================================================
--  C) Archive_cleaned: rooms
-- ===================================================================
UPDATE archive_cleaned.rooms AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM cleaned.rooms AS c
    WHERE c.room_id = a.room_id
      AND (
          c.name        IS DISTINCT FROM a.name
       OR c.location_id IS DISTINCT FROM a.location_id
      )
  );

INSERT INTO archive_cleaned.rooms (
    start_date, end_date, process, source, updated,
    room_id, name, location_id
)
SELECT
    c.updated_at,             -- start_date
    '9999-12-31',             -- end_date
    'scd2_cleaned',           -- process
    'cleaned.rooms',          -- source
    c.updated_at,             -- updated
    c.room_id,
    c.name,
    c.location_id
FROM cleaned.rooms AS c
LEFT JOIN archive_cleaned.rooms AS a
  ON a.room_id = c.room_id
 AND a.end_date = '9999-12-31'
WHERE a.room_id IS NULL
   OR c.name        IS DISTINCT FROM a.name
   OR c.location_id IS DISTINCT FROM a.location_id;


-- ===================================================================
--  D) Archive_cleaned: devices
-- ===================================================================
UPDATE archive_cleaned.devices AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM cleaned.devices AS c
    WHERE c.device_id = a.device_id
      AND (
          c.name     IS DISTINCT FROM a.name
       OR c.category IS DISTINCT FROM a.category
      )
  );

INSERT INTO archive_cleaned.devices (
    start_date, end_date, process, source, updated,
    device_id, name, category
)
SELECT
    c.updated_at,            -- start_date
    '9999-12-31',            -- end_date
    'scd2_cleaned',          -- process
    'cleaned.devices',       -- source
    c.updated_at,            -- updated
    c.device_id,
    c.name,
    c.category
FROM cleaned.devices AS c
LEFT JOIN archive_cleaned.devices AS a
  ON a.device_id = c.device_id
 AND a.end_date = '9999-12-31'
WHERE a.device_id IS NULL
   OR c.name     IS DISTINCT FROM a.name
   OR c.category IS DISTINCT FROM a.category;


-- ===================================================================
--  E) Archive_cleaned: smart_plugs
-- ===================================================================
UPDATE archive_cleaned.smart_plugs AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM cleaned.smart_plugs AS c
    WHERE c.plug_id = a.plug_id
      AND (
          c.user_id   IS DISTINCT FROM a.user_id
       OR c.room_id   IS DISTINCT FROM a.room_id
       OR c.device_id IS DISTINCT FROM a.device_id
       OR c.status    IS DISTINCT FROM a.status
      )
  );

INSERT INTO archive_cleaned.smart_plugs (
    start_date, end_date, process, source, updated,
    plug_id, user_id, room_id, device_id, status
)
SELECT
    c.updated_at,            -- start_date
    '9999-12-31',            -- end_date
    'scd2_cleaned',          -- process
    'cleaned.smart_plugs',   -- source
    c.updated_at,            -- updated
    c.plug_id,
    c.user_id,
    c.room_id,
    c.device_id,
    c.status
FROM cleaned.smart_plugs AS c
LEFT JOIN archive_cleaned.smart_plugs AS a
  ON a.plug_id = c.plug_id
 AND a.end_date = '9999-12-31'
WHERE a.plug_id IS NULL
   OR c.user_id   IS DISTINCT FROM a.user_id
   OR c.room_id   IS DISTINCT FROM a.room_id
   OR c.device_id IS DISTINCT FROM a.device_id
   OR c.status    IS DISTINCT FROM a.status;


-- ===================================================================
--  F) Archive_cleaned: plug_assignments
-- ===================================================================
UPDATE archive_cleaned.plug_assignments AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM cleaned.plug_assignments AS c
    WHERE c.assignment_id = a.assignment_id
      AND (
          c.plug_id    IS DISTINCT FROM a.plug_id
       OR c.room_id    IS DISTINCT FROM a.room_id
       OR c.device_id  IS DISTINCT FROM a.device_id
       OR c.start_time IS DISTINCT FROM a.start_time
       OR c.end_time   IS DISTINCT FROM a.end_time
      )
  );

INSERT INTO archive_cleaned.plug_assignments (
    start_date, end_date, process, source, updated,
    assignment_id, plug_id, room_id, device_id, start_time, end_time
)
SELECT
    c.updated_at,                  -- start_date
    '9999-12-31',                  -- end_date
    'scd2_cleaned',                -- process
    'cleaned.plug_assignments',    -- source
    c.updated_at,                  -- updated
    c.assignment_id,
    c.plug_id,
    c.room_id,
    c.device_id,
    c.start_time,
    c.end_time
FROM cleaned.plug_assignments AS c
LEFT JOIN archive_cleaned.plug_assignments AS a
  ON a.assignment_id = c.assignment_id
 AND a.end_date = '9999-12-31'
WHERE a.assignment_id IS NULL
   OR c.plug_id    IS DISTINCT FROM a.plug_id
   OR c.room_id    IS DISTINCT FROM a.room_id
   OR c.device_id  IS DISTINCT FROM a.device_id
   OR c.start_time IS DISTINCT FROM a.start_time
   OR c.end_time   IS DISTINCT FROM a.end_time;


-- ===================================================================
--  G) Archive_cleaned: readings
-- ===================================================================
UPDATE archive_cleaned.readings AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM cleaned.readings AS c
    WHERE c.plug_id = a.plug_id
      AND c.timestamp = a.timestamp
      AND (
          c.power_kwh IS DISTINCT FROM a.power_kwh
      )
      AND a.end_date = '9999-12-31'
  );

INSERT INTO archive_cleaned.readings (
    start_date, end_date, process, source, updated,
    plug_id, timestamp, power_kwh
)
SELECT
    c.updated_at,           -- start_date
    '9999-12-31',           -- end_date
    'scd2_cleaned',         -- process
    'cleaned.readings',     -- source
    c.updated_at,           -- updated
    c.plug_id,
    c.timestamp,
    c.power_kwh
FROM cleaned.readings AS c
LEFT JOIN archive_cleaned.readings AS a
  ON c.plug_id = a.plug_id
 AND c.timestamp = a.timestamp
 AND a.end_date = '9999-12-31'
WHERE a.plug_id IS NULL
   OR a.timestamp IS NULL
   OR c.power_kwh IS DISTINCT FROM a.power_kwh;


-- ===================================================================
--  H) Archive_cleaned: companies
-- ===================================================================
UPDATE archive_cleaned.companies AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM cleaned.companies AS c
    WHERE c.company_id = a.company_id
      AND (
          c.name     IS DISTINCT FROM a.name
       OR c.industry IS DISTINCT FROM a.industry
      )
  );

INSERT INTO archive_cleaned.companies (
    start_date, end_date, process, source, updated,
    company_id, name, industry
)
SELECT
    c.updated_at,           -- start_date
    '9999-12-31',           -- end_date
    'scd2_cleaned',         -- process
    'cleaned.companies',    -- source
    c.updated_at,           -- updated
    c.company_id,
    c.name,
    c.industry
FROM cleaned.companies AS c
LEFT JOIN archive_cleaned.companies AS a
  ON a.company_id = c.company_id
 AND a.end_date = '9999-12-31'
WHERE a.company_id IS NULL
   OR c.name     IS DISTINCT FROM a.name
   OR c.industry IS DISTINCT FROM a.industry;


-- ===================================================================
--  I) Archive_cleaned: company_locations
-- ===================================================================
UPDATE archive_cleaned.company_locations AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM cleaned.company_locations AS c
    WHERE c.company_location_id = a.company_location_id
      AND (
          c.name       IS DISTINCT FROM a.name
       OR c.country    IS DISTINCT FROM a.country
       OR c.co2_factor IS DISTINCT FROM a.co2_factor
       OR c.company_id IS DISTINCT FROM a.company_id
      )
  );

INSERT INTO archive_cleaned.company_locations (
    start_date, end_date, process, source, updated,
    company_location_id, name, country, co2_factor, company_id
)
SELECT
    c.updated_at,                   -- start_date
    '9999-12-31',                   -- end_date
    'scd2_cleaned',                 -- process
    'cleaned.company_locations',    -- source
    c.updated_at,                   -- updated
    c.company_location_id,
    c.name,
    c.country,
    c.co2_factor,
    c.company_id
FROM cleaned.company_locations AS c
LEFT JOIN archive_cleaned.company_locations AS a
  ON a.company_location_id = c.company_location_id
 AND a.end_date = '9999-12-31'
WHERE a.company_location_id IS NULL
   OR c.name       IS DISTINCT FROM a.name
   OR c.country    IS DISTINCT FROM a.country
   OR c.co2_factor IS DISTINCT FROM a.co2_factor
   OR c.company_id IS DISTINCT FROM a.company_id;


-- ===================================================================
--  J) Archive_cleaned: departments
-- ===================================================================
UPDATE archive_cleaned.departments AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM cleaned.departments AS c
    WHERE c.department_id = a.department_id
      AND (
          c.company_id IS DISTINCT FROM a.company_id
       OR c.name       IS DISTINCT FROM a.name
      )
  );

INSERT INTO archive_cleaned.departments (
    start_date, end_date, process, source, updated,
    department_id, company_id, name
)
SELECT
    c.updated_at,                    -- start_date
    '9999-12-31',                    -- end_date
    'scd2_cleaned',                  -- process
    'cleaned.departments',           -- source
    c.updated_at,                    -- updated
    c.department_id,
    c.company_id,
    c.name
FROM cleaned.departments AS c
LEFT JOIN archive_cleaned.departments AS a
  ON a.department_id = c.department_id
 AND a.end_date = '9999-12-31'
WHERE a.department_id IS NULL
   OR c.company_id IS DISTINCT FROM a.company_id
   OR c.name       IS DISTINCT FROM a.name;


-- ===================================================================
--  K) Archive_cleaned: company_rooms
-- ===================================================================
UPDATE archive_cleaned.company_rooms AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM cleaned.company_rooms AS c
    WHERE c.company_room_id = a.company_room_id
      AND (
          c.name        IS DISTINCT FROM a.name
       OR c.location_id IS DISTINCT FROM a.location_id
      )
  );

INSERT INTO archive_cleaned.company_rooms (
    start_date, end_date, process, source, updated,
    company_room_id, name, location_id
)
SELECT
    c.updated_at,                   -- start_date
    '9999-12-31',                   -- end_date
    'scd2_cleaned',                 -- process
    'cleaned.company_rooms',        -- source
    c.updated_at,                   -- updated
    c.company_room_id,
    c.name,
    c.location_id
FROM cleaned.company_rooms AS c
LEFT JOIN archive_cleaned.company_rooms AS a
  ON a.company_room_id = c.company_room_id
 AND a.end_date = '9999-12-31'
WHERE a.company_room_id IS NULL
   OR c.name        IS DISTINCT FROM a.name
   OR c.location_id IS DISTINCT FROM a.location_id;


-- ===================================================================
--  L) Archive_cleaned: company_devices
-- ===================================================================
UPDATE archive_cleaned.company_devices AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM cleaned.company_devices AS c
    WHERE c.device_id = a.device_id
      AND (
          c.name     IS DISTINCT FROM a.name
       OR c.category IS DISTINCT FROM a.category
      )
  );

INSERT INTO archive_cleaned.company_devices (
    start_date, end_date, process, source, updated,
    device_id, name, category
)
SELECT
    c.updated_at,                -- start_date
    '9999-12-31',                -- end_date
    'scd2_cleaned',              -- process
    'cleaned.company_devices',   -- source
    c.updated_at,                -- updated
    c.device_id,
    c.name,
    c.category
FROM cleaned.company_devices AS c
LEFT JOIN archive_cleaned.company_devices AS a
  ON a.device_id = c.device_id
 AND a.end_date = '9999-12-31'
WHERE a.device_id IS NULL
   OR c.name     IS DISTINCT FROM a.name
   OR c.category IS DISTINCT FROM a.category;


-- ===================================================================
--  M) Archive_cleaned: company_users
-- ===================================================================
UPDATE archive_cleaned.company_users AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM cleaned.company_users AS c
    WHERE c.user_id = a.user_id
      AND (
          c.company_id   IS DISTINCT FROM a.company_id
       OR c.name         IS DISTINCT FROM a.name
       OR c.email        IS DISTINCT FROM a.email
       OR c.department_id IS DISTINCT FROM a.department_id
       OR c.role         IS DISTINCT FROM a.role
      )
  );

INSERT INTO archive_cleaned.company_users (
    start_date, end_date, process, source, updated,
    user_id, company_id, name, email, department_id, role
)
SELECT
    c.updated_at,               -- start_date
    '9999-12-31',               -- end_date
    'scd2_cleaned',             -- process
    'cleaned.company_users',    -- source
    c.updated_at,               -- updated
    c.user_id,
    c.company_id,
    c.name,
    c.email,
    c.department_id,
    c.role
FROM cleaned.company_users AS c
LEFT JOIN archive_cleaned.company_users AS a
  ON a.user_id = c.user_id
 AND a.end_date = '9999-12-31'
WHERE a.user_id IS NULL
   OR c.company_id   IS DISTINCT FROM a.company_id
   OR c.name         IS DISTINCT FROM a.name
   OR c.email        IS DISTINCT FROM a.email
   OR c.department_id IS DISTINCT FROM a.department_id
   OR c.role         IS DISTINCT FROM a.role;


-- ===================================================================
--  N) Archive_cleaned: company_smart_plugs
-- ===================================================================
UPDATE archive_cleaned.company_smart_plugs AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM cleaned.company_smart_plugs AS c
    WHERE c.plug_id = a.plug_id
      AND (
          c.company_id IS DISTINCT FROM a.company_id
       OR c.room_id    IS DISTINCT FROM a.room_id
       OR c.device_id  IS DISTINCT FROM a.device_id
       OR c.status     IS DISTINCT FROM a.status
      )
  );

INSERT INTO archive_cleaned.company_smart_plugs (
    start_date, end_date, process, source, updated,
    plug_id, company_id, room_id, device_id, status
)
SELECT
    c.updated_at,                 -- start_date
    '9999-12-31',                 -- end_date
    'scd2_cleaned',               -- process
    'cleaned.company_smart_plugs',-- source
    c.updated_at,                 -- updated
    c.plug_id,
    c.company_id,
    c.room_id,
    c.device_id,
    c.status
FROM cleaned.company_smart_plugs AS c
LEFT JOIN archive_cleaned.company_smart_plugs AS a
  ON a.plug_id = c.plug_id
 AND a.end_date = '9999-12-31'
WHERE a.plug_id IS NULL
   OR c.company_id IS DISTINCT FROM a.company_id
   OR c.room_id    IS DISTINCT FROM a.room_id
   OR c.device_id  IS DISTINCT FROM a.device_id
   OR c.status     IS DISTINCT FROM a.status;


-- ===================================================================
--  O) Archive_cleaned: company_plug_assignments
-- ===================================================================
UPDATE archive_cleaned.company_plug_assignments AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM cleaned.company_plug_assignments AS c
    WHERE c.company_plug_assignment_id = a.company_plug_assignment_id
      AND (
          c.plug_id    IS DISTINCT FROM a.plug_id
       OR c.room_id    IS DISTINCT FROM a.room_id
       OR c.device_id  IS DISTINCT FROM a.device_id
       OR c.start_time IS DISTINCT FROM a.start_time
       OR c.end_time   IS DISTINCT FROM a.end_time
      )
  );

INSERT INTO archive_cleaned.company_plug_assignments (
    start_date, end_date, process, source, updated,
    company_plug_assignment_id, plug_id, room_id, device_id, start_time, end_time
)
SELECT
    c.updated_at,                         -- start_date
    '9999-12-31',                         -- end_date
    'scd2_cleaned',                       -- process
    'cleaned.company_plug_assignments',   -- source
    c.updated_at,                         -- updated
    c.company_plug_assignment_id,
    c.plug_id,
    c.room_id,
    c.device_id,
    c.start_time,
    c.end_time
FROM cleaned.company_plug_assignments AS c
LEFT JOIN archive_cleaned.company_plug_assignments AS a
  ON a.company_plug_assignment_id = c.company_plug_assignment_id
 AND a.end_date = '9999-12-31'
WHERE a.company_plug_assignment_id IS NULL
   OR c.plug_id    IS DISTINCT FROM a.plug_id
   OR c.room_id    IS DISTINCT FROM a.room_id
   OR c.device_id  IS DISTINCT FROM a.device_id
   OR c.start_time IS DISTINCT FROM a.start_time
   OR c.end_time   IS DISTINCT FROM a.end_time;


-- ===================================================================
--  P) Archive_cleaned: company_readings
-- ===================================================================
UPDATE archive_cleaned.company_readings AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM cleaned.company_readings AS c
    WHERE c.plug_id = a.plug_id
      AND c.timestamp = a.timestamp
      AND (
          c.power_kwh IS DISTINCT FROM a.power_kwh
      )
      AND a.end_date = '9999-12-31'
  );

INSERT INTO archive_cleaned.company_readings (
    start_date, end_date, process, source, updated,
    plug_id, timestamp, power_kwh
)
SELECT
    c.updated_at,               -- start_date
    '9999-12-31',               -- end_date
    'scd2_cleaned',             -- process
    'cleaned.company_readings', -- source
    c.updated_at,               -- updated
    c.plug_id,
    c.timestamp,
    c.power_kwh
FROM cleaned.company_readings AS c
LEFT JOIN archive_cleaned.company_readings AS a
  ON c.plug_id = a.plug_id
 AND c.timestamp = a.timestamp
 AND a.end_date = '9999-12-31'
WHERE a.plug_id IS NULL
   OR a.timestamp IS NULL
   OR c.power_kwh IS DISTINCT FROM a.power_kwh;


-- ===================================================================
-- Archive_CLEANED: co2_factors
-- ===================================================================
-- Archive_CLEANED: co2_factors
UPDATE archive_cleaned.co2_factors AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1 FROM cleaned.co2_factors c
     WHERE c.source_name=a.source_name
       AND c.country=a.country
       AND (c.co2_factor IS DISTINCT FROM a.co2_factor OR c.unit IS DISTINCT FROM a.unit)
  );
INSERT INTO archive_cleaned.co2_factors
  (start_date,end_date,process,source,updated,source_name,country,co2_factor,unit)
SELECT c.updated_at,'9999-12-31','scd2_cleaned','cleaned.co2_factors',c.updated_at,
       c.source_name,c.country,c.co2_factor,c.unit
  FROM cleaned.co2_factors c
  LEFT JOIN archive_cleaned.co2_factors a
    ON a.source_name=c.source_name AND a.country=c.country AND a.end_date='9999-12-31'
 WHERE a.source_name IS NULL
    OR c.co2_factor IS DISTINCT FROM a.co2_factor
    OR c.unit       IS DISTINCT FROM a.unit;




-- ===================================================================
-- Archive_CLEANED: electricity_prices
-- ===================================================================
-- 1) Zatvaramo stare verzije
-- Archive_CLEANED: electricity_prices
UPDATE archive_cleaned.electricity_prices AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1 FROM cleaned.electricity_prices c
     WHERE c.country=a.country
       AND c.price_per_kwh IS DISTINCT FROM a.price_per_kwh
  );
INSERT INTO archive_cleaned.electricity_prices
  (start_date,end_date,process,source,updated,country,price_per_kwh)
SELECT c.updated_at,'9999-12-31','scd2_cleaned','cleaned.electricity_prices',c.updated_at,
       c.country,c.price_per_kwh
  FROM cleaned.electricity_prices c
  LEFT JOIN archive_cleaned.electricity_prices a
    ON a.country=c.country AND a.end_date='9999-12-31'
 WHERE a.country IS NULL
    OR c.price_per_kwh IS DISTINCT FROM a.price_per_kwh;




-- ================================================================
--  Kraj SCD2 logike za cleaned → archive_cleaned
-- ================================================================
