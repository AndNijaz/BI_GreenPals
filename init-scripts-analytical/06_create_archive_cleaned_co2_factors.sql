CREATE SCHEMA IF NOT EXISTS archive_cleaned;

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
