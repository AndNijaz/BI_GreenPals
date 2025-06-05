# CREATE ARCHIVE SCHEMA FULL SCRIPT

```sql
-- 1) Kreiraj shemu archive (ako već ne postoji)
CREATE SCHEMA IF NOT EXISTS archive;

-- 2) Tablica archive.users
CREATE TABLE IF NOT EXISTS archive.users (
  id SERIAL PRIMARY KEY,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process TEXT NOT NULL,
  source TEXT NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  user_id UUID,
  name TEXT,
  email TEXT,
  eco_points INTEGER
);

-- 3) Tablica archive.devices
CREATE TABLE IF NOT EXISTS archive.devices (
  id SERIAL PRIMARY KEY,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process TEXT NOT NULL,
  source TEXT NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  device_id INTEGER,
  name TEXT,
  category TEXT
);

-- 4) Tablica archive.smart_plugs
CREATE TABLE IF NOT EXISTS archive.smart_plugs (
  id SERIAL PRIMARY KEY,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process TEXT NOT NULL,
  source TEXT NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  plug_id UUID,
  user_id UUID,
  room_id INTEGER,
  device_id INTEGER,
  status BOOLEAN
);

-- 5) Tablica archive.plug_assignments
CREATE TABLE IF NOT EXISTS archive.plug_assignments (
  id SERIAL PRIMARY KEY,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process TEXT NOT NULL,
  source TEXT NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  assignment_id INTEGER,
  plug_id UUID,
  room_id INTEGER,
  device_id INTEGER,
  start_time TIMESTAMP,
  end_time TIMESTAMP
);

-- 6) Tablica archive.readings
CREATE TABLE IF NOT EXISTS archive.readings (
  id SERIAL PRIMARY KEY,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process TEXT NOT NULL,
  source TEXT NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  plug_id UUID,
  timestamp TIMESTAMP,
  power_kwh DECIMAL(6,3)
);

-- 7) Tablica archive.company_users
CREATE TABLE IF NOT EXISTS archive.company_users (
  id SERIAL PRIMARY KEY,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process TEXT NOT NULL,
  source TEXT NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  user_id UUID,
  company_id UUID,
  name TEXT,
  email TEXT,
  department_id INTEGER,
  role TEXT
);

-- 8) Tablica archive.company_devices
CREATE TABLE IF NOT EXISTS archive.company_devices (
  id SERIAL PRIMARY KEY,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process TEXT NOT NULL,
  source TEXT NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  device_id INTEGER,
  name TEXT,
  category TEXT
);

-- 9) Tablica archive.company_readings
CREATE TABLE IF NOT EXISTS archive.company_readings (
  id SERIAL PRIMARY KEY,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process TEXT NOT NULL,
  source TEXT NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  plug_id UUID,
  timestamp TIMESTAMP,
  power_kwh DECIMAL(6,3)
);

-- 10) Tablica archive.company_smart_plugs
CREATE TABLE IF NOT EXISTS archive.company_smart_plugs (
  id SERIAL PRIMARY KEY,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process TEXT NOT NULL,
  source TEXT NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  plug_id UUID,
  company_id UUID,
  room_id INTEGER,
  device_id INTEGER,
  status BOOLEAN
);

-- 11) Tablica archive.companies
CREATE TABLE IF NOT EXISTS archive.companies (
  id SERIAL PRIMARY KEY,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process TEXT NOT NULL,
  source TEXT NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  company_id UUID,
  name TEXT,
  industry TEXT
);

-- 12) Tablica archive.locations
CREATE TABLE IF NOT EXISTS archive.locations (
  id SERIAL PRIMARY KEY,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process TEXT NOT NULL,
  source TEXT NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  location_id INTEGER,
  name TEXT,
  country TEXT
);

-- 13) Tablica archive.rooms
CREATE TABLE IF NOT EXISTS archive.rooms (
  id SERIAL PRIMARY KEY,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process TEXT NOT NULL,
  source TEXT NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  room_id INTEGER,
  name TEXT,
  location_id INTEGER
);

-- 14) Tablica archive.company_locations
CREATE TABLE IF NOT EXISTS archive.company_locations (
  id SERIAL PRIMARY KEY,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process TEXT NOT NULL,
  source TEXT NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  company_location_id INTEGER,
  name TEXT,
  country TEXT,
  co2_factor DECIMAL,
  company_id UUID
);

-- 15) Tablica archive.company_rooms
CREATE TABLE IF NOT EXISTS archive.company_rooms (
  id SERIAL PRIMARY KEY,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process TEXT NOT NULL,
  source TEXT NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  company_room_id INTEGER,
  name TEXT,
  location_id INTEGER
);

-- 16) Tablica archive.departments
CREATE TABLE IF NOT EXISTS archive.departments (
  id SERIAL PRIMARY KEY,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process TEXT NOT NULL,
  source TEXT NOT NULL,
  updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  department_id INTEGER,
  company_id UUID,
  name TEXT
);

-- 17) Indeksi radi brzog lookupa “aktivnih” verzija
CREATE INDEX IF NOT EXISTS idx_archive_users_lookup
  ON archive.users (user_id, end_date);
CREATE INDEX IF NOT EXISTS idx_archive_devices_lookup
  ON archive.devices (device_id, end_date);
CREATE INDEX IF NOT EXISTS idx_archive_smart_plugs_lookup
  ON archive.smart_plugs (plug_id, end_date);
CREATE INDEX IF NOT EXISTS idx_archive_plug_assignments_lookup
  ON archive.plug_assignments (assignment_id, end_date);
CREATE INDEX IF NOT EXISTS idx_archive_readings_lookup
  ON archive.readings (plug_id, timestamp, end_date);
CREATE INDEX IF NOT EXISTS idx_archive_company_users_lookup
  ON archive.company_users (user_id, end_date);
CREATE INDEX IF NOT EXISTS idx_archive_company_devices_lookup
  ON archive.company_devices (device_id, end_date);
CREATE INDEX IF NOT EXISTS idx_archive_company_readings_lookup
  ON archive.company_readings (plug_id, timestamp, end_date);
CREATE INDEX IF NOT EXISTS idx_archive_company_smart_plugs_lookup
  ON archive.company_smart_plugs (plug_id, end_date);
CREATE INDEX IF NOT EXISTS idx_archive_companies_lookup
  ON archive.companies (company_id, end_date);
CREATE INDEX IF NOT EXISTS idx_archive_locations_lookup
  ON archive.locations (location_id, end_date);
CREATE INDEX IF NOT EXISTS idx_archive_rooms_lookup
  ON archive.rooms (room_id, end_date);
CREATE INDEX IF NOT EXISTS idx_archive_company_locations_lookup
  ON archive.company_locations (company_location_id, end_date);
CREATE INDEX IF NOT EXISTS idx_archive_company_rooms_lookup
  ON archive.company_rooms (company_room_id, end_date);
CREATE INDEX IF NOT EXISTS idx_archive_departments_lookup
  ON archive.departments (department_id, end_date);
```
