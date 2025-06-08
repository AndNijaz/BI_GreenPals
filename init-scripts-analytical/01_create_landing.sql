-- 01_create_landing.sql

-- 1) Kreiraj shemu landing
CREATE SCHEMA IF NOT EXISTS landing;

-- 2) Landing tablice koje odgovaraju public shemi iz db_operational

CREATE TABLE IF NOT EXISTS landing.users (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE NOT NULL,
  created_at TIMESTAMP NOT NULL,
  eco_points INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.locations (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  country TEXT NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.rooms (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  location_id INTEGER NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.devices (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  category TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.smart_plugs (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL,
  room_id INTEGER NOT NULL,
  device_id INTEGER NOT NULL,
  status BOOLEAN NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.plug_assignments (
  id INTEGER PRIMARY KEY,
  plug_id UUID NOT NULL,
  room_id INTEGER NOT NULL,
  device_id INTEGER NOT NULL,
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.readings (
  id INTEGER PRIMARY KEY,
  plug_id UUID NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  power_kwh DECIMAL(6,3) NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

-- 3) Landing tablice koje odgovaraju company_schema shemi iz db_operational

CREATE TABLE IF NOT EXISTS landing.companies (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  industry TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.company_locations (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  country TEXT NOT NULL,
  co2_factor DECIMAL NOT NULL,
  company_id UUID NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.departments (
  id INTEGER PRIMARY KEY,
  company_id UUID NOT NULL,
  name TEXT NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.company_rooms (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  location_id INTEGER NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.company_devices (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  category TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.company_users (
  id UUID PRIMARY KEY,
  company_id UUID NOT NULL,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  department_id INTEGER,
  role TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.company_smart_plugs (
  id UUID PRIMARY KEY,
  company_id UUID NOT NULL,
  room_id INTEGER NOT NULL,
  device_id INTEGER NOT NULL,
  status BOOLEAN NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.company_plug_assignments (
  id INTEGER PRIMARY KEY,
  plug_id UUID NOT NULL,
  room_id INTEGER NOT NULL,
  device_id INTEGER NOT NULL,
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS landing.company_readings (
  id INTEGER PRIMARY KEY,
  plug_id UUID NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  power_kwh DECIMAL(6,3) NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

-- CREATE TABLE IF NOT EXISTS landing.co2_factors (
--   source_name TEXT      NOT NULL,
--   country     TEXT      NOT NULL,
--   co2_factor  DECIMAL(10,5) NOT NULL,
--   unit        TEXT      DEFAULT 'kgCO2/kWh',
--   updated_at  TIMESTAMP NOT NULL
-- );

-- -- landing.electricity_prices
-- CREATE TABLE IF NOT EXISTS landing.electricity_prices (
--   country        TEXT      PRIMARY KEY,
--   price_per_kwh  DECIMAL(6,3) NOT NULL,
--   updated_at     TIMESTAMP  NOT NULL DEFAULT CURRENT_TIMESTAMP
-- );

-- 1) CO2 FACTORS -----------------------------------------------------
CREATE TABLE IF NOT EXISTS landing.co2_factors (
    source_name TEXT   NOT NULL,
    country     TEXT   NOT NULL,
    co2_factor  DECIMAL(10,5) NOT NULL,
    unit        TEXT   DEFAULT 'kgCO2/kWh',
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_name, country)                 -- <-- PK!
);

-- 2) ELECTRICITY PRICES ---------------------------------------------
CREATE TABLE IF NOT EXISTS landing.electricity_prices (
    country        TEXT      NOT NULL,
    price_per_kwh  DECIMAL(6,3) NOT NULL,
    updated_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (country)                            -- <-- PK!
);