-- 02_create_cleaned.sql

-- 1) Kreiraj shemu cleaned
CREATE SCHEMA IF NOT EXISTS cleaned;

--------------------------------------------------------------------------------
-- Tablice u cleaned koje odgovaraju landing.* (isto polja, ali s validacijom)

CREATE TABLE IF NOT EXISTS cleaned.users (
  user_id     UUID      PRIMARY KEY,
  name        TEXT      NOT NULL,
  email       TEXT      NOT NULL,
  eco_points  INTEGER   NOT NULL,
  created_at  TIMESTAMP,       -- sada NULLABLE
  updated_at  TIMESTAMP NOT NULL
  -- (dodatni stupci za validaciju, ako želite npr. is_valid_email BOOLEAN, ...)
);

CREATE TABLE IF NOT EXISTS cleaned.locations (
  location_id INTEGER   PRIMARY KEY,
  name        TEXT      NOT NULL,
  country     TEXT      NOT NULL,
  updated_at  TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS cleaned.rooms (
  room_id     INTEGER   PRIMARY KEY,
  name        TEXT      NOT NULL,
  location_id INTEGER   NOT NULL,
  updated_at  TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS cleaned.devices (
  device_id   INTEGER   PRIMARY KEY,
  name        TEXT      NOT NULL,
  category    TEXT      NOT NULL,
  created_at  TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS cleaned.smart_plugs (
  plug_id     UUID      PRIMARY KEY,
  user_id     UUID      NOT NULL,
  room_id     INTEGER   NOT NULL,
  device_id   INTEGER   NOT NULL,
  status      BOOLEAN   NOT NULL,
  created_at  TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS cleaned.plug_assignments (
  assignment_id INTEGER   PRIMARY KEY,
  plug_id       UUID      NOT NULL,
  room_id       INTEGER   NOT NULL,
  device_id     INTEGER   NOT NULL,
  start_time    TIMESTAMP NOT NULL,
  end_time      TIMESTAMP,
  created_at    TIMESTAMP,
  updated_at    TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS cleaned.readings (
  plug_id     UUID      NOT NULL,
  timestamp   TIMESTAMP NOT NULL,
  power_kwh   DECIMAL(6,3) NOT NULL,
  created_at  TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL,
  PRIMARY KEY (plug_id, timestamp)
);

--------------------------------------------------------------------------------
-- Company dio

CREATE TABLE IF NOT EXISTS cleaned.companies (
  company_id  UUID      PRIMARY KEY,
  name        TEXT      NOT NULL,
  industry    TEXT      NOT NULL,
  created_at  TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS cleaned.company_locations (
  company_location_id INTEGER   PRIMARY KEY,
  name               TEXT      NOT NULL,
  country            TEXT      NOT NULL,
  co2_factor         DECIMAL   NOT NULL,
  company_id         UUID      NOT NULL,
  updated_at         TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS cleaned.departments (
  department_id INTEGER   PRIMARY KEY,
  company_id    UUID      NOT NULL,
  name          TEXT      NOT NULL,
  updated_at    TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS cleaned.company_rooms (
  company_room_id INTEGER   PRIMARY KEY,
  name            TEXT      NOT NULL,
  location_id     INTEGER   NOT NULL,
  updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS cleaned.company_devices (
  device_id    INTEGER   PRIMARY KEY,
  name         TEXT      NOT NULL,
  category     TEXT      NOT NULL,
  created_at   TIMESTAMP,
  updated_at   TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS cleaned.company_users (
  user_id       UUID      PRIMARY KEY,
  company_id    UUID      NOT NULL,
  name          TEXT      NOT NULL,
  email         TEXT      NOT NULL,
  department_id INTEGER,
  role          TEXT      NOT NULL,
  created_at    TIMESTAMP,
  updated_at    TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS cleaned.company_smart_plugs (
  plug_id     UUID      PRIMARY KEY,
  company_id  UUID      NOT NULL,
  room_id     INTEGER   NOT NULL,
  device_id   INTEGER   NOT NULL,
  status      BOOLEAN   NOT NULL,
  created_at  TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS cleaned.company_plug_assignments (
  company_plug_assignment_id INTEGER   PRIMARY KEY,
  plug_id                     UUID      NOT NULL,
  room_id                     INTEGER   NOT NULL,
  device_id                   INTEGER   NOT NULL,
  start_time                  TIMESTAMP NOT NULL,
  end_time                    TIMESTAMP,
  created_at                  TIMESTAMP,
  updated_at                  TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS cleaned.company_readings (
  plug_id     UUID      NOT NULL,
  timestamp   TIMESTAMP NOT NULL,
  power_kwh   DECIMAL(6,3) NOT NULL,
  created_at  TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL,
  PRIMARY KEY (plug_id, timestamp)
);

CREATE TABLE IF NOT EXISTS cleaned.co2_factors (
  source_name    TEXT      NOT NULL,
  country        TEXT      NOT NULL,
  co2_factor     NUMERIC(10,5) NOT NULL,
  unit           TEXT      NOT NULL,
  updated_at     TIMESTAMP NOT NULL,
  PRIMARY KEY (source_name, country)
);

CREATE TABLE IF NOT EXISTS cleaned.electricity_prices (
  country        TEXT      PRIMARY KEY,
  price_per_kwh  DECIMAL(6,3) NOT NULL,
  updated_at     TIMESTAMP NOT NULL
);