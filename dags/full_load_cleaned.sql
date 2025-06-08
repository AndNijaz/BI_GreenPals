-- full_load_cleaned.sql

-- 1) Truncate svih cleaned.* tablica
TRUNCATE
  cleaned.users,
  cleaned.locations,
  cleaned.rooms,
  cleaned.devices,
  cleaned.smart_plugs,
  cleaned.plug_assignments,
  cleaned.readings,
  cleaned.co2_factors,
  cleaned.electricity_prices,
  cleaned.companies,
  cleaned.company_locations,
  cleaned.departments,
  cleaned.company_rooms,
  cleaned.company_devices,
  cleaned.company_users,
  cleaned.company_smart_plugs,
  cleaned.company_plug_assignments,
  cleaned.company_readings
RESTART IDENTITY CASCADE;

-- 2) cleaned.users
INSERT INTO cleaned.users (user_id, name, email, eco_points, created_at, updated_at)
SELECT user_id, name, email, eco_points, NULL, updated
FROM archive.users
WHERE end_date = '9999-12-31';

-- 3) cleaned.locations
INSERT INTO cleaned.locations (location_id, name, country, updated_at)
SELECT location_id, name, country, updated
FROM archive.locations
WHERE end_date = '9999-12-31';

-- 4) cleaned.rooms
INSERT INTO cleaned.rooms (room_id, name, location_id, updated_at)
SELECT room_id, name, location_id, updated
FROM archive.rooms
WHERE end_date = '9999-12-31';

-- 5) cleaned.devices
INSERT INTO cleaned.devices (device_id, name, category, created_at, updated_at)
SELECT device_id, name, category, NULL, updated
FROM archive.devices
WHERE end_date = '9999-12-31';

-- 6) cleaned.smart_plugs
INSERT INTO cleaned.smart_plugs (plug_id, user_id, room_id, device_id, status, created_at, updated_at)
SELECT plug_id, user_id, room_id, device_id, status, NULL, updated
FROM archive.smart_plugs
WHERE end_date = '9999-12-31';

-- 7) cleaned.plug_assignments
INSERT INTO cleaned.plug_assignments
  (assignment_id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at)
SELECT assignment_id, plug_id, room_id, device_id, start_time, end_time, NULL, updated
FROM archive.plug_assignments
WHERE end_date = '9999-12-31';

-- 8) cleaned.readings (DISTINCT ON)
INSERT INTO cleaned.readings (plug_id, "timestamp", power_kwh, created_at, updated_at)
SELECT plug_id, "timestamp", power_kwh, NULL, updated
FROM (
  SELECT DISTINCT ON (plug_id, "timestamp")
    plug_id, "timestamp", power_kwh, updated
  FROM archive.readings
  WHERE end_date = '9999-12-31'
  ORDER BY plug_id, "timestamp", updated DESC
) AS dr;

-- 9) cleaned.co2_factors
INSERT INTO cleaned.co2_factors (source_name, country, co2_factor, unit, updated_at)
SELECT source_name, country, co2_factor, unit, updated
FROM archive.co2_factors
WHERE end_date = '9999-12-31';

-- 10) cleaned.electricity_prices
INSERT INTO cleaned.electricity_prices (country, price_per_kwh, updated_at)
SELECT country, price_per_kwh, updated
FROM archive.electricity_prices
WHERE end_date = '9999-12-31';

-- 11) cleaned.companies
INSERT INTO cleaned.companies (company_id, name, industry, created_at, updated_at)
SELECT company_id, name, industry, NULL, updated
FROM archive.companies
WHERE end_date = '9999-12-31';

-- 12) cleaned.company_locations
INSERT INTO cleaned.company_locations
  (company_location_id, name, country, co2_factor, company_id, updated_at)
SELECT company_location_id, name, country, co2_factor, company_id, updated
FROM archive.company_locations
WHERE end_date = '9999-12-31';

-- 13) cleaned.departments
INSERT INTO cleaned.departments (department_id, company_id, name, updated_at)
SELECT department_id, company_id, name, updated
FROM archive.departments
WHERE end_date = '9999-12-31';

-- 14) cleaned.company_rooms
INSERT INTO cleaned.company_rooms (company_room_id, name, location_id, updated_at)
SELECT company_room_id, name, location_id, updated
FROM archive.company_rooms
WHERE end_date = '9999-12-31';

-- 15) cleaned.company_devices
INSERT INTO cleaned.company_devices (device_id, name, category, created_at, updated_at)
SELECT device_id, name, category, NULL, updated
FROM archive.company_devices
WHERE end_date = '9999-12-31';

-- 16) cleaned.company_users
INSERT INTO cleaned.company_users
  (user_id, company_id, name, email, department_id, role, created_at, updated_at)
SELECT user_id, company_id, name, email, department_id, role, NULL, updated
FROM archive.company_users
WHERE end_date = '9999-12-31';

-- 17) cleaned.company_smart_plugs
INSERT INTO cleaned.company_smart_plugs
  (plug_id, company_id, room_id, device_id, status, created_at, updated_at)
SELECT plug_id, company_id, room_id, device_id, status, NULL, updated
FROM archive.company_smart_plugs
WHERE end_date = '9999-12-31';

-- 18) cleaned.company_plug_assignments
INSERT INTO cleaned.company_plug_assignments
  (company_plug_assignment_id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at)
SELECT company_plug_assignment_id, plug_id, room_id, device_id, start_time, end_time, NULL, updated
FROM archive.company_plug_assignments
WHERE end_date = '9999-12-31';

-- 19) cleaned.company_readings (DISTINCT ON)
INSERT INTO cleaned.company_readings (plug_id, "timestamp", power_kwh, created_at, updated_at)
SELECT plug_id, "timestamp", power_kwh, NULL, updated
FROM (
  SELECT DISTINCT ON (plug_id, "timestamp")
    plug_id, "timestamp", power_kwh, updated
  FROM archive.company_readings
  WHERE end_date = '9999-12-31'
  ORDER BY plug_id, "timestamp", updated DESC
) AS dcr;
