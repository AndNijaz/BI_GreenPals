# FULL LOAD SCRIPT

```sql
-- ================================================================
-- FULL LOAD I SCD2 AŽURIRANJE ZA SVE TABELE
-- Pretpostavka: Postoje landing.* tabele (s PRIMARY KEY) i archive.* tabele
-- za sljedeće entitete:
--   users, devices, smart_plugs, plug_assignments, readings,
--   company_users, company_devices, company_readings, company_smart_plugs,
--   companies, locations, rooms, company_locations, company_rooms, departments
-- Archive tabele imaju polja:
--   start_date TIMESTAMP, end_date TIMESTAMP, process TEXT, source TEXT, updated TIMESTAMP,
--   plus kolone specifične za entitet (poput user_id, device_id itd.)
-- ================================================================

-- 1) TRUNCATE svih landing tabela i RESTART IDENTITY (ako je primjenjivo)
TRUNCATE landing.devices,
         landing.locations,
         landing.plug_assignments,
         landing.readings,
         landing.rooms,
         landing.smart_plugs,
         landing.users,
         landing.company_devices,
         landing.company_locations,
         landing.company_plug_assignments,
         landing.company_readings,
         landing.company_rooms,
         landing.company_smart_plugs,
         landing.company_users,
         landing.companies,
         landing.departments
RESTART IDENTITY;

-- 2) Puni landing tabele iz operativnih (public.* i company_schema.*, departments iz company_schema)
-- PUBLIC za devices
INSERT INTO landing.devices (id, name, category, created_at, updated_at)
SELECT id, name, category, created_at, updated_at
FROM public.devices;

-- PUBLIC za locations
INSERT INTO landing.locations (id, name, country, updated_at)
SELECT id, name, country, updated_at
FROM public.locations;

-- PUBLIC za plug_assignments
INSERT INTO landing.plug_assignments (
    id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at
)
SELECT id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at
FROM public.plug_assignments;

-- PUBLIC za readings
INSERT INTO landing.readings (
    id, plug_id, timestamp, power_kwh, created_at, updated_at
)
SELECT id, plug_id, timestamp, power_kwh, created_at, updated_at
FROM public.readings;

-- PUBLIC za rooms
INSERT INTO landing.rooms (id, name, location_id, updated_at)
SELECT id, name, location_id, updated_at
FROM public.rooms;

-- PUBLIC za smart_plugs
INSERT INTO landing.smart_plugs (
    id, user_id, room_id, device_id, status, created_at, updated_at
)
SELECT id, user_id, room_id, device_id, status, created_at, updated_at
FROM public.smart_plugs;

-- PUBLIC za users
INSERT INTO landing.users (
    id, name, email, eco_points, created_at, updated_at
)
SELECT id, name, email, eco_points, created_at, updated_at
FROM public.users;

-- COMPANY_SCHEMA za company_devices
INSERT INTO landing.company_devices (
    id, name, category, created_at, updated_at
)
SELECT id, name, category, created_at, updated_at
FROM company_schema.company_devices;

-- COMPANY_SCHEMA za company_locations
INSERT INTO landing.company_locations (
    id, name, country, co2_factor, company_id, updated_at
)
SELECT id, name, country, co2_factor, company_id, updated_at
FROM company_schema.company_locations;

-- COMPANY_SCHEMA za company_rooms
INSERT INTO landing.company_rooms (
    id, name, location_id, updated_at
)
SELECT id, name, location_id, updated_at
FROM company_schema.company_rooms;

-- COMPANY_SCHEMA za company_smart_plugs
INSERT INTO landing.company_smart_plugs (
    id, company_id, room_id, device_id, status, created_at, updated_at
)
SELECT id, company_id, room_id, device_id, status, created_at, updated_at
FROM company_schema.company_smart_plugs;

-- COMPANY_SCHEMA za company_users
INSERT INTO landing.company_users (
    id, company_id, name, email, department_id, role, created_at, updated_at
)
SELECT id, company_id, name, email, department_id, role, created_at, updated_at
FROM company_schema.company_users;

-- COMPANY_SCHEMA za company_plug_assignments
INSERT INTO landing.company_plug_assignments (
    id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at
)
SELECT id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at
FROM company_schema.company_plug_assignments;

-- COMPANY_SCHEMA za company_readings
INSERT INTO landing.company_readings (
    id, plug_id, timestamp, power_kwh, created_at, updated_at
)
SELECT id, plug_id, timestamp, power_kwh, created_at, updated_at
FROM company_schema.company_readings;

-- COMPANY_SCHEMA za companies
INSERT INTO landing.companies (
    id, name, industry, created_at, updated_at
)
SELECT id, name, industry, created_at, updated_at
FROM company_schema.companies;

-- COMPANY_SCHEMA za departments
INSERT INTO landing.departments (
    id, company_id, name, updated_at
)
SELECT id, company_id, name, updated_at
FROM company_schema.departments;

-- 3) SCD2 LOGIKA: AŽURIRANJE archive TABELA
-- Napomena: Archive tabele se ne TRUNCATE-a – zadržavamo povijest

-- ========================
-- A) Archive: users
-- ========================
-- 3A.1) Zatvorimo postojeće verzije koje su se promijenile
UPDATE archive.users AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.users AS l
    WHERE l.id = a.user_id
      AND (
          l.name IS DISTINCT FROM a.name
          OR l.email IS DISTINCT FROM a.email
          OR l.eco_points IS DISTINCT FROM a.eco_points
      )
  );

-- 3A.2) Ubacimo nove verzije za promijenjene ili nove korisnike
INSERT INTO archive.users (
    start_date, end_date, process, source, updated,
    user_id, name, email, eco_points
)
SELECT
    l.updated_at, '9999-12-31', 'scd2_update', 'landing.users', l.updated_at,
    l.id, l.name, l.email, l.eco_points
FROM landing.users AS l
LEFT JOIN archive.users AS a
  ON l.id = a.user_id
 AND a.end_date = '9999-12-31'
WHERE a.user_id IS NULL
   OR l.name IS DISTINCT FROM a.name
   OR l.email IS DISTINCT FROM a.email
   OR l.eco_points IS DISTINCT FROM a.eco_points;

-- ========================
-- B) Archive: devices
-- ========================
-- 3B.1) Zatvorimo postojeće verzije uređaja koji su se promijenili
UPDATE archive.devices AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.devices AS l
    WHERE l.id = a.device_id
      AND (
          l.name IS DISTINCT FROM a.name
          OR l.category IS DISTINCT FROM a.category
      )
  );

-- 3B.2) Ubacimo nove verzije za promijenjene ili nove uređaje
INSERT INTO archive.devices (
    start_date, end_date, process, source, updated,
    device_id, name, category
)
SELECT
    l.updated_at, '9999-12-31', 'scd2_update', 'landing.devices', l.updated_at,
    l.id, l.name, l.category
FROM landing.devices AS l
LEFT JOIN archive.devices AS a
  ON l.id = a.device_id
 AND a.end_date = '9999-12-31'
WHERE a.device_id IS NULL
   OR l.name IS DISTINCT FROM a.name
   OR l.category IS DISTINCT FROM a.category;

-- ========================
-- C) Archive: smart_plugs
-- ========================
-- 3C.1) Zatvorimo postojeće verzije smart_plug-a koji su se promijenili
UPDATE archive.smart_plugs AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.smart_plugs AS l
    WHERE l.id = a.plug_id
      AND (
          l.user_id IS DISTINCT FROM a.user_id
          OR l.room_id IS DISTINCT FROM a.room_id
          OR l.device_id IS DISTINCT FROM a.device_id
          OR l.status IS DISTINCT FROM a.status
      )
  );

-- 3C.2) Ubacimo nove verzije za promijenjene ili nove smart_plug-ove
INSERT INTO archive.smart_plugs (
    start_date, end_date, process, source, updated,
    plug_id, user_id, room_id, device_id, status
)
SELECT
    l.updated_at, '9999-12-31', 'scd2_update', 'landing.smart_plugs', l.updated_at,
    l.id, l.user_id, l.room_id, l.device_id, l.status
FROM landing.smart_plugs AS l
LEFT JOIN archive.smart_plugs AS a
  ON l.id = a.plug_id
 AND a.end_date = '9999-12-31'
WHERE a.plug_id IS NULL
   OR l.user_id IS DISTINCT FROM a.user_id
   OR l.room_id IS DISTINCT FROM a.room_id
   OR l.device_id IS DISTINCT FROM a.device_id
   OR l.status IS DISTINCT FROM a.status;

-- ========================
-- D) Archive: plug_assignments
-- ========================
-- 3D.1) Zatvorimo postojeće verzije dodjela koje su se promijenile
UPDATE archive.plug_assignments AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.plug_assignments AS l
    WHERE l.id = a.assignment_id
      AND (
          l.plug_id IS DISTINCT FROM a.plug_id
          OR l.room_id IS DISTINCT FROM a.room_id
          OR l.device_id IS DISTINCT FROM a.device_id
          OR l.start_time IS DISTINCT FROM a.start_time
          OR l.end_time IS DISTINCT FROM a.end_time
      )
  );

-- 3D.2) Ubacimo nove verzije za promijenjene ili nove plug_assignments
INSERT INTO archive.plug_assignments (
    start_date, end_date, process, source, updated,
    assignment_id, plug_id, room_id, device_id, start_time, end_time
)
SELECT
    l.updated_at, '9999-12-31', 'scd2_update', 'landing.plug_assignments', l.updated_at,
    l.id, l.plug_id, l.room_id, l.device_id, l.start_time, l.end_time
FROM landing.plug_assignments AS l
LEFT JOIN archive.plug_assignments AS a
  ON l.id = a.assignment_id
 AND a.end_date = '9999-12-31'
WHERE a.assignment_id IS NULL
   OR l.plug_id IS DISTINCT FROM a.plug_id
   OR l.room_id IS DISTINCT FROM a.room_id
   OR l.device_id IS DISTINCT FROM a.device_id
   OR l.start_time IS DISTINCT FROM a.start_time
   OR l.end_time IS DISTINCT FROM a.end_time;

-- ========================
-- E) Archive: readings
-- ========================
-- 3E.1) Zatvorimo postojeće verzije očitanja koja su se promijenila
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

-- 3E.2) Ubacimo nove verzije za promijenjena ili nova očitanja
INSERT INTO archive.readings (
    start_date, end_date, process, source, updated,
    plug_id, timestamp, power_kwh
)
SELECT
    l.updated_at, '9999-12-31', 'scd2_update', 'landing.readings', l.updated_at,
    l.plug_id, l.timestamp, l.power_kwh
FROM landing.readings AS l
LEFT JOIN archive.readings AS a
  ON l.plug_id = a.plug_id
 AND l.timestamp = a.timestamp
 AND a.end_date = '9999-12-31'
WHERE a.plug_id IS NULL
   OR a.timestamp IS NULL
   OR l.power_kwh IS DISTINCT FROM a.power_kwh;

-- ================================
-- F) Archive: company_users
-- ================================
-- 3F.1) Zatvorimo postojeće verzije company_users koje su se promijenile
UPDATE archive.company_users AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.company_users AS l
    WHERE l.id = a.user_id
      AND (
          l.company_id IS DISTINCT FROM a.company_id
          OR l.name IS DISTINCT FROM a.name
          OR l.email IS DISTINCT FROM a.email
          OR l.department_id IS DISTINCT FROM a.department_id
          OR l.role IS DISTINCT FROM a.role
      )
  );

-- 3F.2) Ubacimo nove verzije za promijenjene ili nove company_users
INSERT INTO archive.company_users (
    start_date, end_date, process, source, updated,
    user_id, company_id, name, email, department_id, role
)
SELECT
    l.updated_at, '9999-12-31', 'scd2_update', 'landing.company_users', l.updated_at,
    l.id, l.company_id, l.name, l.email, l.department_id, l.role
FROM landing.company_users AS l
LEFT JOIN archive.company_users AS a
  ON l.id = a.user_id
 AND a.end_date = '9999-12-31'
WHERE a.user_id IS NULL
   OR l.company_id IS DISTINCT FROM a.company_id
   OR l.name IS DISTINCT FROM a.name
   OR l.email IS DISTINCT FROM a.email
   OR l.department_id IS DISTINCT FROM a.department_id
   OR l.role IS DISTINCT FROM a.role;

-- ================================
-- G) Archive: company_devices
-- ================================
-- 3G.1) Zatvorimo postojeće verzije company_devices koje su se promijenile
UPDATE archive.company_devices AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.company_devices AS l
    WHERE l.id = a.device_id
      AND (
          l.name IS DISTINCT FROM a.name
          OR l.category IS DISTINCT FROM a.category
      )
  );

-- 3G.2) Ubacimo nove verzije za promijenjene ili nove company_devices
INSERT INTO archive.company_devices (
    start_date, end_date, process, source, updated,
    device_id, name, category
)
SELECT
    l.updated_at, '9999-12-31', 'scd2_update', 'landing.company_devices', l.updated_at,
    l.id, l.name, l.category
FROM landing.company_devices AS l
LEFT JOIN archive.company_devices AS a
  ON l.id = a.device_id
 AND a.end_date = '9999-12-31'
WHERE a.device_id IS NULL
   OR l.name IS DISTINCT FROM a.name
   OR l.category IS DISTINCT FROM a.category;

-- ==================================
-- H) Archive: company_readings
-- ==================================
-- 3H.1) Zatvorimo postojeća očitanja u company_readings koja su se promijenila
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

-- 3H.2) Ubacimo nove verzije za promijenjena ili nova očitanja u company_readings
INSERT INTO archive.company_readings (
    start_date, end_date, process, source, updated,
    plug_id, timestamp, power_kwh
)
SELECT
    l.updated_at, '9999-12-31', 'scd2_update', 'landing.company_readings', l.updated_at,
    l.plug_id, l.timestamp, l.power_kwh
FROM landing.company_readings AS l
LEFT JOIN archive.company_readings AS a
  ON l.plug_id = a.plug_id
 AND l.timestamp = a.timestamp
 AND a.end_date = '9999-12-31'
WHERE a.plug_id IS NULL
   OR a.timestamp IS NULL
   OR l.power_kwh IS DISTINCT FROM a.power_kwh;

-- ========================================
-- I) Archive: company_smart_plugs
-- ========================================
-- 3I.1) Zatvorimo postojeće verzije company_smart_plugs koje su se promijenile
UPDATE archive.company_smart_plugs AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.company_smart_plugs AS l
    WHERE l.id = a.plug_id
      AND (
          l.company_id IS DISTINCT FROM a.company_id
          OR l.room_id IS DISTINCT FROM a.room_id
          OR l.device_id IS DISTINCT FROM a.device_id
          OR l.status IS DISTINCT FROM a.status
      )
  );

-- 3I.2) Ubacimo nove verzije za promijenjene ili nove company_smart_plugs
INSERT INTO archive.company_smart_plugs (
    start_date, end_date, process, source, updated,
    plug_id, company_id, room_id, device_id, status
)
SELECT
    l.updated_at, '9999-12-31', 'scd2_update', 'landing.company_smart_plugs', l.updated_at,
    l.id, l.company_id, l.room_id, l.device_id, l.status
FROM landing.company_smart_plugs AS l
LEFT JOIN archive.company_smart_plugs AS a
  ON l.id = a.plug_id
 AND a.end_date = '9999-12-31'
WHERE a.plug_id IS NULL
   OR l.company_id IS DISTINCT FROM a.company_id
   OR l.room_id IS DISTINCT FROM a.room_id
   OR l.device_id IS DISTINCT FROM a.device_id
   OR l.status IS DISTINCT FROM a.status;

-- ================================
-- J) Archive: companies
-- ================================
-- 3J.1) Zatvorimo postojeće verzije companies koje su se promijenile
UPDATE archive.companies AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.companies AS l
    WHERE l.id = a.company_id
      AND (
          l.name IS DISTINCT FROM a.name
          OR l.industry IS DISTINCT FROM a.industry
      )
  );

-- 3J.2) Ubacimo nove verzije za promijenjene ili nove kompanije
INSERT INTO archive.companies (
    start_date, end_date, process, source, updated,
    company_id, name, industry
)
SELECT
    l.updated_at, '9999-12-31', 'scd2_update', 'landing.companies', l.updated_at,
    l.id, l.name, l.industry
FROM landing.companies AS l
LEFT JOIN archive.companies AS a
  ON l.id = a.company_id
 AND a.end_date = '9999-12-31'
WHERE a.company_id IS NULL
   OR l.name IS DISTINCT FROM a.name
   OR l.industry IS DISTINCT FROM a.industry;

-- ========================================
-- K) Archive: locations
-- ========================================
-- 3K.1) Zatvorimo postojeće verzije locations koje su se promijenile
UPDATE archive.locations AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.locations AS l
    WHERE l.id = a.location_id
      AND (
          l.name IS DISTINCT FROM a.name
          OR l.country IS DISTINCT FROM a.country
      )
  );

-- 3K.2) Ubacimo nove verzije za promijenjene ili nove locations
INSERT INTO archive.locations (
    start_date, end_date, process, source, updated,
    location_id, name, country
)
SELECT
    l.updated_at, '9999-12-31', 'scd2_update', 'landing.locations', l.updated_at,
    l.id, l.name, l.country
FROM landing.locations AS l
LEFT JOIN archive.locations AS a
  ON l.id = a.location_id
 AND a.end_date = '9999-12-31'
WHERE a.location_id IS NULL
   OR l.name IS DISTINCT FROM a.name
   OR l.country IS DISTINCT FROM a.country;

-- ========================================
-- L) Archive: rooms
-- ========================================
-- 3L.1) Zatvorimo postojeće verzije rooms koje su se promijenile
UPDATE archive.rooms AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.rooms AS l
    WHERE l.id = a.room_id
      AND (
          l.name IS DISTINCT FROM a.name
          OR l.location_id IS DISTINCT FROM a.location_id
      )
  );

-- 3L.2) Ubacimo nove verzije za promijenjene ili nove rooms
INSERT INTO archive.rooms (
    start_date, end_date, process, source, updated,
    room_id, name, location_id
)
SELECT
    l.updated_at, '9999-12-31', 'scd2_update', 'landing.rooms', l.updated_at,
    l.id, l.name, l.location_id
FROM landing.rooms AS l
LEFT JOIN archive.rooms AS a
  ON l.id = a.room_id
 AND a.end_date = '9999-12-31'
WHERE a.room_id IS NULL
   OR l.name IS DISTINCT FROM a.name
   OR l.location_id IS DISTINCT FROM a.location_id;

-- ================================================
-- M) Archive: company_locations
-- ================================================
-- 3M.1) Zatvorimo postojeće verzije company_locations koje su se promijenile
UPDATE archive.company_locations AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.company_locations AS l
    WHERE l.id = a.company_location_id
      AND (
          l.name IS DISTINCT FROM a.name
          OR l.country IS DISTINCT FROM a.country
          OR l.co2_factor IS DISTINCT FROM a.co2_factor
          OR l.company_id IS DISTINCT FROM a.company_id
      )
  );

-- 3M.2) Ubacimo nove verzije za promijenjene ili nove company_locations
INSERT INTO archive.company_locations (
    start_date, end_date, process, source, updated,
    company_location_id, name, country, co2_factor, company_id
)
SELECT
    l.updated_at, '9999-12-31', 'scd2_update', 'landing.company_locations', l.updated_at,
    l.id, l.name, l.country, l.co2_factor, l.company_id
FROM landing.company_locations AS l
LEFT JOIN archive.company_locations AS a
  ON l.id = a.company_location_id
 AND a.end_date = '9999-12-31'
WHERE a.company_location_id IS NULL
   OR l.name IS DISTINCT FROM a.name
   OR l.country IS DISTINCT FROM a.country
   OR l.co2_factor IS DISTINCT FROM a.co2_factor
   OR l.company_id IS DISTINCT FROM a.company_id;

-- ================================================
-- N) Archive: company_rooms
-- ================================================
-- 3N.1) Zatvorimo postojeće verzije company_rooms koje su se promijenile
UPDATE archive.company_rooms AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.company_rooms AS l
    WHERE l.id = a.company_room_id
      AND (
          l.name IS DISTINCT FROM a.name
          OR l.location_id IS DISTINCT FROM a.location_id
      )
  );

-- 3N.2) Ubacimo nove verzije za promijenjene ili nove company_rooms
INSERT INTO archive.company_rooms (
    start_date, end_date, process, source, updated,
    company_room_id, name, location_id
)
SELECT
    l.updated_at, '9999-12-31', 'scd2_update', 'landing.company_rooms', l.updated_at,
    l.id, l.name, l.location_id
FROM landing.company_rooms AS l
LEFT JOIN archive.company_rooms AS a
  ON l.id = a.company_room_id
 AND a.end_date = '9999-12-31'
WHERE a.company_room_id IS NULL
   OR l.name IS DISTINCT FROM a.name
   OR l.location_id IS DISTINCT FROM a.location_id;

-- ================================================
-- O) Archive: departments
-- ================================================
-- 3O.1) Zatvorimo postojeće verzije departments koje su se promijenile
UPDATE archive.departments AS a
SET end_date = CURRENT_TIMESTAMP
WHERE a.end_date = '9999-12-31'
  AND EXISTS (
    SELECT 1
    FROM landing.departments AS l
    WHERE l.id = a.department_id
      AND (
          l.company_id IS DISTINCT FROM a.company_id
          OR l.name IS DISTINCT FROM a.name
      )
  );

-- 3O.2) Ubacimo nove verzije za promijenjene ili nove departments
INSERT INTO archive.departments (
    start_date, end_date, process, source, updated,
    department_id, company_id, name
)
SELECT
    l.updated_at, '9999-12-31', 'scd2_update', 'landing.departments', l.updated_at,
    l.id, l.company_id, l.name
FROM landing.departments AS l
LEFT JOIN archive.departments AS a
  ON l.id = a.department_id
 AND a.end_date = '9999-12-31'
WHERE a.department_id IS NULL
   OR l.company_id IS DISTINCT FROM a.company_id
   OR l.name IS DISTINCT FROM a.name;

-- ================================================================
-- Kraj SCD2 logike
-- ================================================================
```
