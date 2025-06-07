-- 05_create_co2_factors.sql
CREATE SCHEMA IF NOT EXISTS archive;

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

CREATE INDEX IF NOT EXISTS idx_archive_co2_factors_lookup
  ON archive.co2_factors(source_name, end_date);
