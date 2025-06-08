-- 03_create_archive_cleaned.sql

-- 1) Kreiraj shemu archive_cleaned
CREATE SCHEMA IF NOT EXISTS archive_cleaned;

--------------------------------------------------------------------------------
-- Archive_CLEANED: isti SCD2-stil kao što ste radili za archive_raw,
-- ali polja su iz „cleaned“ sheme (aktivni trenutačni i povijesni).
-- Evo primjer za users; ostale tablice su analogne.

CREATE TABLE IF NOT EXISTS archive_cleaned.users (
    id           SERIAL PRIMARY KEY,
    start_date   TIMESTAMP   NOT NULL,
    end_date     TIMESTAMP   NOT NULL DEFAULT '9999-12-31',
    process      TEXT        NOT NULL,
    source       TEXT        NOT NULL,
    updated      TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    user_id      UUID,
    name         TEXT,
    email        TEXT,
    eco_points   INTEGER
);

CREATE INDEX IF NOT EXISTS idx_arch_cln_users_lookup
    ON archive_cleaned.users (user_id, end_date);

--------------------------------------------------------------------------------
-- Archive_CLEANED: locations

CREATE TABLE IF NOT EXISTS archive_cleaned.locations (
    id            SERIAL PRIMARY KEY,
    start_date    TIMESTAMP   NOT NULL,
    end_date      TIMESTAMP   NOT NULL DEFAULT '9999-12-31',
    process       TEXT        NOT NULL,
    source        TEXT        NOT NULL,
    updated       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    location_id   INTEGER,
    name          TEXT,
    country       TEXT
);
CREATE INDEX IF NOT EXISTS idx_arch_cln_locations_lookup
    ON archive_cleaned.locations (location_id, end_date);

--------------------------------------------------------------------------------
-- Archive_CLEANED: rooms

CREATE TABLE IF NOT EXISTS archive_cleaned.rooms (
    id            SERIAL PRIMARY KEY,
    start_date    TIMESTAMP   NOT NULL,
    end_date      TIMESTAMP   NOT NULL DEFAULT '9999-12-31',
    process       TEXT        NOT NULL,
    source        TEXT        NOT NULL,
    updated       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    room_id       INTEGER,
    name          TEXT,
    location_id   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_arch_cln_rooms_lookup
    ON archive_cleaned.rooms (room_id, end_date);

--------------------------------------------------------------------------------
-- Archive_CLEANED: devices

CREATE TABLE IF NOT EXISTS archive_cleaned.devices (
    id            SERIAL PRIMARY KEY,
    start_date    TIMESTAMP   NOT NULL,
    end_date      TIMESTAMP   NOT NULL DEFAULT '9999-12-31',
    process       TEXT        NOT NULL,
    source        TEXT        NOT NULL,
    updated       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    device_id     INTEGER,
    name          TEXT,
    category      TEXT
);
CREATE INDEX IF NOT EXISTS idx_arch_cln_devices_lookup
    ON archive_cleaned.devices (device_id, end_date);

--------------------------------------------------------------------------------
-- Archive_CLEANED: smart_plugs

CREATE TABLE IF NOT EXISTS archive_cleaned.smart_plugs (
    id            SERIAL PRIMARY KEY,
    start_date    TIMESTAMP   NOT NULL,
    end_date      TIMESTAMP   NOT NULL DEFAULT '9999-12-31',
    process       TEXT        NOT NULL,
    source        TEXT        NOT NULL,
    updated       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    plug_id       UUID,
    user_id       UUID,
    room_id       INTEGER,
    device_id     INTEGER,
    status        BOOLEAN
);
CREATE INDEX IF NOT EXISTS idx_arch_cln_smart_plugs_lookup
    ON archive_cleaned.smart_plugs (plug_id, end_date);

--------------------------------------------------------------------------------
-- Archive_CLEANED: plug_assignments

CREATE TABLE IF NOT EXISTS archive_cleaned.plug_assignments (
    id                 SERIAL PRIMARY KEY,
    start_date         TIMESTAMP   NOT NULL,
    end_date           TIMESTAMP   NOT NULL DEFAULT '9999-12-31',
    process            TEXT        NOT NULL,
    source             TEXT        NOT NULL,
    updated            TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    assignment_id      INTEGER,
    plug_id            UUID,
    room_id            INTEGER,
    device_id          INTEGER,
    start_time         TIMESTAMP,
    end_time           TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_arch_cln_plug_assignments_lookup
    ON archive_cleaned.plug_assignments (assignment_id, end_date);

--------------------------------------------------------------------------------
-- Archive_CLEANED: readings

CREATE TABLE IF NOT EXISTS archive_cleaned.readings (
    id            SERIAL PRIMARY KEY,
    start_date    TIMESTAMP   NOT NULL,
    end_date      TIMESTAMP   NOT NULL DEFAULT '9999-12-31',
    process       TEXT        NOT NULL,
    source        TEXT        NOT NULL,
    updated       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    plug_id       UUID,
    timestamp     TIMESTAMP,
    power_kwh     DECIMAL(6,3)
);
CREATE INDEX IF NOT EXISTS idx_arch_cln_readings_lookup
    ON archive_cleaned.readings (plug_id, timestamp, end_date);

--------------------------------------------------------------------------------
-- Company dio

CREATE TABLE IF NOT EXISTS archive_cleaned.companies (
    id             SERIAL PRIMARY KEY,
    start_date     TIMESTAMP   NOT NULL,
    end_date       TIMESTAMP   NOT NULL DEFAULT '9999-12-31',
    process        TEXT        NOT NULL,
    source         TEXT        NOT NULL,
    updated        TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    company_id     UUID,
    name           TEXT,
    industry       TEXT
);
CREATE INDEX IF NOT EXISTS idx_arch_cln_companies_lookup
    ON archive_cleaned.companies (company_id, end_date);

CREATE TABLE IF NOT EXISTS archive_cleaned.company_locations (
    id                 SERIAL PRIMARY KEY,
    start_date         TIMESTAMP   NOT NULL,
    end_date           TIMESTAMP   NOT NULL DEFAULT '9999-12-31',
    process            TEXT        NOT NULL,
    source             TEXT        NOT NULL,
    updated            TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    company_location_id INTEGER,
    name               TEXT,
    country            TEXT,
    co2_factor         DECIMAL,
    company_id         UUID
);
CREATE INDEX IF NOT EXISTS idx_arch_cln_company_locations_lookup
    ON archive_cleaned.company_locations (company_location_id, end_date);

CREATE TABLE IF NOT EXISTS archive_cleaned.departments (
    id             SERIAL PRIMARY KEY,
    start_date     TIMESTAMP   NOT NULL,
    end_date       TIMESTAMP   NOT NULL DEFAULT '9999-12-31',
    process        TEXT        NOT NULL,
    source         TEXT        NOT NULL,
    updated        TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    department_id  INTEGER,
    company_id     UUID,
    name           TEXT
);
CREATE INDEX IF NOT EXISTS idx_arch_cln_departments_lookup
    ON archive_cleaned.departments (department_id, end_date);

CREATE TABLE IF NOT EXISTS archive_cleaned.company_rooms (
    id               SERIAL PRIMARY KEY,
    start_date       TIMESTAMP   NOT NULL,
    end_date         TIMESTAMP   NOT NULL DEFAULT '9999-12-31',
    process          TEXT        NOT NULL,
    source           TEXT        NOT NULL,
    updated          TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    company_room_id  INTEGER,
    name             TEXT,
    location_id      INTEGER
);
CREATE INDEX IF NOT EXISTS idx_arch_cln_company_rooms_lookup
    ON archive_cleaned.company_rooms (company_room_id, end_date);

CREATE TABLE IF NOT EXISTS archive_cleaned.company_devices (
    id            SERIAL PRIMARY KEY,
    start_date    TIMESTAMP   NOT NULL,
    end_date      TIMESTAMP   NOT NULL DEFAULT '9999-12-31',
    process       TEXT        NOT NULL,
    source        TEXT        NOT NULL,
    updated       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    device_id     INTEGER,
    name          TEXT,
    category      TEXT
);
CREATE INDEX IF NOT EXISTS idx_arch_cln_company_devices_lookup
    ON archive_cleaned.company_devices (device_id, end_date);

CREATE TABLE IF NOT EXISTS archive_cleaned.company_users (
    id            SERIAL PRIMARY KEY,
    start_date    TIMESTAMP   NOT NULL,
    end_date      TIMESTAMP   NOT NULL DEFAULT '9999-12-31',
    process       TEXT        NOT NULL,
    source        TEXT        NOT NULL,
    updated       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    user_id       UUID,
    company_id    UUID,
    name          TEXT,
    email         TEXT,
    department_id INTEGER,
    role          TEXT
);
CREATE INDEX IF NOT EXISTS idx_arch_cln_company_users_lookup
    ON archive_cleaned.company_users (user_id, end_date);

CREATE TABLE IF NOT EXISTS archive_cleaned.company_smart_plugs (
    id            SERIAL PRIMARY KEY,
    start_date    TIMESTAMP   NOT NULL,
    end_date      TIMESTAMP   NOT NULL DEFAULT '9999-12-31',
    process       TEXT        NOT NULL,
    source        TEXT        NOT NULL,
    updated       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    plug_id       UUID,
    company_id    UUID,
    room_id       INTEGER,
    device_id     INTEGER,
    status        BOOLEAN
);
CREATE INDEX IF NOT EXISTS idx_arch_cln_company_smart_plugs_lookup
    ON archive_cleaned.company_smart_plugs (plug_id, end_date);

CREATE TABLE IF NOT EXISTS archive_cleaned.company_plug_assignments (
    id                      SERIAL PRIMARY KEY,
    start_date              TIMESTAMP   NOT NULL,
    end_date                TIMESTAMP   NOT NULL DEFAULT '9999-12-31',
    process                 TEXT        NOT NULL,
    source                  TEXT        NOT NULL,
    updated                 TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    company_plug_assignment_id INTEGER,
    plug_id                  UUID,
    room_id                  INTEGER,
    device_id                INTEGER,
    start_time               TIMESTAMP,
    end_time                 TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_arch_cln_company_plug_assignments_lookup
    ON archive_cleaned.company_plug_assignments (company_plug_assignment_id, end_date);

CREATE TABLE IF NOT EXISTS archive_cleaned.company_readings (
    id            SERIAL PRIMARY KEY,
    start_date    TIMESTAMP   NOT NULL,
    end_date      TIMESTAMP   NOT NULL DEFAULT '9999-12-31',
    process       TEXT        NOT NULL,
    source        TEXT        NOT NULL,
    updated       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    plug_id       UUID,
    timestamp     TIMESTAMP,
    power_kwh     DECIMAL(6,3)
);
CREATE INDEX IF NOT EXISTS idx_arch_cln_company_readings_lookup
    ON archive_cleaned.company_readings (plug_id, timestamp, end_date);

CREATE TABLE IF NOT EXISTS archive_cleaned.electricity_prices (
  id           SERIAL PRIMARY KEY,
  start_date   TIMESTAMP NOT NULL,
  end_date     TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process      TEXT      NOT NULL,
  source       TEXT      NOT NULL,
  updated      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  country      TEXT,
  price_per_kwh DECIMAL(6,3)
);

CREATE INDEX IF NOT EXISTS idx_arch_cln_electricity_prices_active
  ON archive_cleaned.electricity_prices(country, end_date)
  WHERE end_date = '9999-12-31';

CREATE TABLE IF NOT EXISTS archive_cleaned.co2_factors (
  id           SERIAL PRIMARY KEY,
  start_date   TIMESTAMP NOT NULL,
  end_date     TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process      TEXT      NOT NULL,
  source       TEXT      NOT NULL,
  updated      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  source_name  TEXT,
  country      TEXT,
  co2_factor   DECIMAL(10,5),
  unit         TEXT
);

CREATE INDEX IF NOT EXISTS idx_arch_cln_co2_factors_lookup
  ON archive_cleaned.co2_factors(source_name, end_date);
