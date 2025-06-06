-- 1) Definicija funkcije koja će ažurirati updated_at
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2) Kreiranje sheme company_schema (ako već ne postoji)
CREATE SCHEMA IF NOT EXISTS company_schema;

-- 3) Tablica company_schema.companies
CREATE TABLE IF NOT EXISTS company_schema.companies (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  industry TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_companies_updated_at
BEFORE UPDATE ON company_schema.companies
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 4) Tablica company_schema.company_locations
CREATE TABLE IF NOT EXISTS company_schema.company_locations (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  country TEXT NOT NULL,
  co2_factor DECIMAL NOT NULL,
  company_id UUID NOT NULL REFERENCES company_schema.companies(id) ON DELETE CASCADE,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_company_locations_updated_at
BEFORE UPDATE ON company_schema.company_locations
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 5) Tablica company_schema.departments
CREATE TABLE IF NOT EXISTS company_schema.departments (
  id SERIAL PRIMARY KEY,
  company_id UUID NOT NULL REFERENCES company_schema.companies(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_departments_updated_at
BEFORE UPDATE ON company_schema.departments
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 6) Tablica company_schema.company_rooms
CREATE TABLE IF NOT EXISTS company_schema.company_rooms (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  location_id INTEGER NOT NULL REFERENCES company_schema.company_locations(id) ON DELETE CASCADE,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_company_rooms_updated_at
BEFORE UPDATE ON company_schema.company_rooms
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 7) Tablica company_schema.company_devices
CREATE TABLE IF NOT EXISTS company_schema.company_devices (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  category TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_company_devices_updated_at
BEFORE UPDATE ON company_schema.company_devices
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 8) Tablica company_schema.company_users
CREATE TABLE IF NOT EXISTS company_schema.company_users (
  id UUID PRIMARY KEY,
  company_id UUID NOT NULL REFERENCES company_schema.companies(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  department_id INTEGER REFERENCES company_schema.departments(id) ON DELETE SET NULL,
  role TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_company_users_updated_at
BEFORE UPDATE ON company_schema.company_users
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 9) Tablica company_schema.company_smart_plugs
CREATE TABLE IF NOT EXISTS company_schema.company_smart_plugs (
  id UUID PRIMARY KEY,
  company_id UUID NOT NULL REFERENCES company_schema.companies(id) ON DELETE CASCADE,
  room_id INTEGER NOT NULL REFERENCES company_schema.company_rooms(id) ON DELETE CASCADE,
  device_id INTEGER NOT NULL REFERENCES company_schema.company_devices(id) ON DELETE CASCADE,
  status BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_company_smart_plugs_updated_at
BEFORE UPDATE ON company_schema.company_smart_plugs
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 10) Tablica company_schema.company_plug_assignments
CREATE TABLE IF NOT EXISTS company_schema.company_plug_assignments (
  id SERIAL PRIMARY KEY,
  plug_id UUID NOT NULL REFERENCES company_schema.company_smart_plugs(id) ON DELETE CASCADE,
  room_id INTEGER NOT NULL REFERENCES company_schema.company_rooms(id) ON DELETE CASCADE,
  device_id INTEGER NOT NULL REFERENCES company_schema.company_devices(id) ON DELETE CASCADE,
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_company_plug_assignments_updated_at
BEFORE UPDATE ON company_schema.company_plug_assignments
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 11) Tablica company_schema.company_readings
CREATE TABLE IF NOT EXISTS company_schema.company_readings (
  id SERIAL PRIMARY KEY,
  plug_id UUID NOT NULL REFERENCES company_schema.company_smart_plugs(id) ON DELETE CASCADE,
  timestamp TIMESTAMP NOT NULL,
  power_kwh DECIMAL(6,3) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_company_readings_updated_at
BEFORE UPDATE ON company_schema.company_readings
FOR EACH ROW EXECUTE FUNCTION set_updated_at();