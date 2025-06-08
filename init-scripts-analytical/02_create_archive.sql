-- Kreiranje sheme archive
CREATE SCHEMA IF NOT EXISTS archive;

-- Archive: users
CREATE TABLE IF NOT EXISTS archive.users (
    id SERIAL PRIMARY KEY,
    start_date  TIMESTAMP NOT NULL,
    end_date    TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    process     TEXT NOT NULL,
    source      TEXT NOT NULL,
    updated     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    user_id     UUID,
    name        TEXT,
    email       TEXT,
    eco_points  INTEGER
);

-- Archive: locations
CREATE TABLE IF NOT EXISTS archive.locations (
    id SERIAL PRIMARY KEY,
    start_date   TIMESTAMP NOT NULL,
    end_date     TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    process      TEXT NOT NULL,
    source       TEXT NOT NULL,
    updated      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    location_id  INTEGER,
    name         TEXT,
    country      TEXT
);

-- Archive: rooms
CREATE TABLE IF NOT EXISTS archive.rooms (
    id SERIAL PRIMARY KEY,
    start_date   TIMESTAMP NOT NULL,
    end_date     TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    process      TEXT NOT NULL,
    source       TEXT NOT NULL,
    updated      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    room_id      INTEGER,
    name         TEXT,
    location_id  INTEGER
);

-- Archive: devices
CREATE TABLE IF NOT EXISTS archive.devices (
    id SERIAL PRIMARY KEY,
    start_date   TIMESTAMP NOT NULL,
    end_date     TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    process      TEXT NOT NULL,
    source       TEXT NOT NULL,
    updated      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    device_id    INTEGER,
    name         TEXT,
    category     TEXT
);

-- Archive: smart_plugs
CREATE TABLE IF NOT EXISTS archive.smart_plugs (
    id SERIAL PRIMARY KEY,
    start_date   TIMESTAMP NOT NULL,
    end_date     TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    process      TEXT NOT NULL,
    source       TEXT NOT NULL,
    updated      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    plug_id      UUID,
    user_id      UUID,
    room_id      INTEGER,
    device_id    INTEGER,
    status       BOOLEAN
);

-- Archive: plug_assignments
CREATE TABLE IF NOT EXISTS archive.plug_assignments (
    id SERIAL PRIMARY KEY,
    start_date            TIMESTAMP NOT NULL,
    end_date              TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    process               TEXT NOT NULL,
    source                TEXT NOT NULL,
    updated               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    assignment_id         INTEGER,
    plug_id               UUID,
    room_id               INTEGER,
    device_id             INTEGER,
    start_time            TIMESTAMP,
    end_time              TIMESTAMP
);

-- Archive: readings
CREATE TABLE IF NOT EXISTS archive.readings (
    id SERIAL PRIMARY KEY,
    start_date   TIMESTAMP NOT NULL,
    end_date     TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    process      TEXT NOT NULL,
    source       TEXT NOT NULL,
    updated      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    plug_id      UUID,
    timestamp    TIMESTAMP,
    power_kwh    DECIMAL(6,3)
);

-- Archive: companies
CREATE TABLE IF NOT EXISTS archive.companies (
    id SERIAL PRIMARY KEY,
    start_date    TIMESTAMP NOT NULL,
    end_date      TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    process       TEXT NOT NULL,
    source        TEXT NOT NULL,
    updated       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    company_id    UUID,
    name          TEXT,
    industry      TEXT
);

-- Archive: company_locations
CREATE TABLE IF NOT EXISTS archive.company_locations (
    id SERIAL PRIMARY KEY,
    start_date             TIMESTAMP NOT NULL,
    end_date               TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    process                TEXT NOT NULL,
    source                 TEXT NOT NULL,
    updated                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    company_location_id    INTEGER,
    name                   TEXT,
    country                TEXT,
    co2_factor             DECIMAL,
    company_id             UUID
);

-- Archive: departments
CREATE TABLE IF NOT EXISTS archive.departments (
    id SERIAL PRIMARY KEY,
    start_date      TIMESTAMP NOT NULL,
    end_date        TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    process         TEXT NOT NULL,
    source          TEXT NOT NULL,
    updated         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    department_id   INTEGER,
    company_id      UUID,
    name            TEXT
);

-- Archive: company_rooms
CREATE TABLE IF NOT EXISTS archive.company_rooms (
    id SERIAL PRIMARY KEY,
    start_date          TIMESTAMP NOT NULL,
    end_date            TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    process             TEXT NOT NULL,
    source              TEXT NOT NULL,
    updated             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    company_room_id     INTEGER,
    name                TEXT,
    location_id         INTEGER
);

-- Archive: company_devices
CREATE TABLE IF NOT EXISTS archive.company_devices (
    id SERIAL PRIMARY KEY,
    start_date     TIMESTAMP NOT NULL,
    end_date       TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    process        TEXT NOT NULL,
    source         TEXT NOT NULL,
    updated        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    device_id      INTEGER,
    name           TEXT,
    category       TEXT
);

-- Archive: company_users
CREATE TABLE IF NOT EXISTS archive.company_users (
    id SERIAL PRIMARY KEY,
    start_date      TIMESTAMP NOT NULL,
    end_date        TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    process         TEXT NOT NULL,
    source          TEXT NOT NULL,
    updated         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    user_id         UUID,
    company_id      UUID,
    name            TEXT,
    email           TEXT,
    department_id   INTEGER,
    role            TEXT
);

-- Archive: company_smart_plugs
CREATE TABLE IF NOT EXISTS archive.company_smart_plugs (
    id SERIAL PRIMARY KEY,
    start_date     TIMESTAMP NOT NULL,
    end_date       TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    process        TEXT NOT NULL,
    source         TEXT NOT NULL,
    updated        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    plug_id        UUID,
    company_id     UUID,
    room_id        INTEGER,
    device_id      INTEGER,
    status         BOOLEAN
);

-- Archive: company_plug_assignments
CREATE TABLE IF NOT EXISTS archive.company_plug_assignments (
    id SERIAL PRIMARY KEY,
    start_date                 TIMESTAMP NOT NULL,
    end_date                   TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    process                    TEXT NOT NULL,
    source                     TEXT NOT NULL,
    updated                    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    company_plug_assignment_id INTEGER,
    plug_id                    UUID,
    room_id                    INTEGER,
    device_id                  INTEGER,
    start_time                 TIMESTAMP,
    end_time                   TIMESTAMP
);

-- Archive: company_readings
CREATE TABLE IF NOT EXISTS archive.company_readings (
    id SERIAL PRIMARY KEY,
    start_date   TIMESTAMP NOT NULL,
    end_date     TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    process      TEXT NOT NULL,
    source       TEXT NOT NULL,
    updated      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    plug_id      UUID,
    timestamp    TIMESTAMP,
    power_kwh    DECIMAL(6,3)
);


CREATE TABLE IF NOT EXISTS archive.electricity_prices (
  id           SERIAL PRIMARY KEY,
  start_date   TIMESTAMP NOT NULL,
  end_date     TIMESTAMP NOT NULL DEFAULT '9999-12-31',
  process      TEXT      NOT NULL,
  source       TEXT      NOT NULL,
  updated      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  country      TEXT,
  price_per_kwh DECIMAL(6,3)
);

CREATE TABLE IF NOT EXISTS archive.co2_factors (
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

-- Indeksi radi bolje performanse SCD2 update-a (opcionalno)

CREATE INDEX IF NOT EXISTS idx_archive_users_lookup
    ON archive.users (user_id, end_date);

CREATE INDEX IF NOT EXISTS idx_archive_locations_lookup
    ON archive.locations (location_id, end_date);

CREATE INDEX IF NOT EXISTS idx_archive_rooms_lookup
    ON archive.rooms (room_id, end_date);

CREATE INDEX IF NOT EXISTS idx_archive_devices_lookup
    ON archive.devices (device_id, end_date);

CREATE INDEX IF NOT EXISTS idx_archive_smart_plugs_lookup
    ON archive.smart_plugs (plug_id, end_date);

CREATE INDEX IF NOT EXISTS idx_archive_plug_assignments_lookup
    ON archive.plug_assignments (assignment_id, end_date);

CREATE INDEX IF NOT EXISTS idx_archive_readings_lookup
    ON archive.readings (plug_id, timestamp, end_date);

CREATE INDEX IF NOT EXISTS idx_archive_companies_lookup
    ON archive.companies (company_id, end_date);

CREATE INDEX IF NOT EXISTS idx_archive_company_locations_lookup
    ON archive.company_locations (company_location_id, end_date);

CREATE INDEX IF NOT EXISTS idx_archive_departments_lookup
    ON archive.departments (department_id, end_date);

CREATE INDEX IF NOT EXISTS idx_archive_company_rooms_lookup
    ON archive.company_rooms (company_room_id, end_date);

CREATE INDEX IF NOT EXISTS idx_archive_company_devices_lookup
    ON archive.company_devices (device_id, end_date);

CREATE INDEX IF NOT EXISTS idx_archive_company_users_lookup
    ON archive.company_users (user_id, end_date);

CREATE INDEX IF NOT EXISTS idx_archive_company_smart_plugs_lookup
    ON archive.company_smart_plugs (plug_id, end_date);

CREATE INDEX IF NOT EXISTS idx_archive_company_plug_assignments_lookup
    ON archive.company_plug_assignments (company_plug_assignment_id, end_date);

CREATE INDEX IF NOT EXISTS idx_archive_company_readings_lookup
    ON archive.company_readings (plug_id, timestamp, end_date);

CREATE INDEX IF NOT EXISTS idx_archive_electricity_prices_active
    ON archive.electricity_prices (country, end_date)
    WHERE end_date = '9999-12-31';
CREATE INDEX IF NOT EXISTS idx_archive_co2_factors_active
    ON archive.co2_factors (source_name, end_date)
    WHERE end_date = '9999-12-31';