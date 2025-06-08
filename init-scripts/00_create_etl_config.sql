/* ─────────────────────────────────────────────────────────────
   00_create_etl_config.sql           (⚠ NE pokreće se automatski)
   GreenPals / Company IoT projekt – metadata-driven ETL
   ───────────────────────────────────────────────────────────── */

-- 1) Shema za metapodatke
CREATE SCHEMA IF NOT EXISTS metadata;

-- 2) Kontrolna (metadata) tabela
CREATE TABLE IF NOT EXISTS metadata.etl_config (
    id                SERIAL PRIMARY KEY,
    schema_name       VARCHAR(80)  NOT NULL,
    table_name        VARCHAR(120) NOT NULL,
    load_type         VARCHAR(20)  NOT NULL CHECK (load_type IN ('full','incremental')),
    schedule_interval VARCHAR(20)  DEFAULT 'manual',
    last_loaded_at    TIMESTAMP,
    active            BOOLEAN      DEFAULT TRUE,
    UNIQUE(schema_name, table_name)
);

/* 3) Početna konfiguracija svih tablica
   ───────────────────────────────────────────────
   • statične referentne tablice  →  full-load
   • tokovi koji stalno rastu     →  incremental
   • schedule_interval: slobodno promijeni!
*/

INSERT INTO metadata.etl_config
        (schema_name,  table_name,             load_type,   schedule_interval, last_loaded_at, active)
VALUES
/* ---------- PUBLIC shema (individual users) ---------- */
        ('public','users',                 'full',        'daily',   NULL, TRUE),
        ('public','locations',             'full',        'daily',   NULL, TRUE),
        ('public','rooms',                 'full',        'daily',   NULL, TRUE),
        ('public','devices',               'full',        'daily',   NULL, TRUE),
        ('public','smart_plugs',           'incremental', '15min',   NULL, TRUE),
        ('public','plug_assignments',      'incremental', '15min',   NULL, TRUE),
        ('public','readings',              'incremental', '5min',    NULL, TRUE),

/* ---------- COMPANY shema (B2B firmama) --------------- */
        ('company_schema','companies',                'full',        'daily',   NULL, TRUE),
        ('company_schema','company_locations',        'full',        'daily',   NULL, TRUE),
        ('company_schema','departments',              'full',        'daily',   NULL, TRUE),
        ('company_schema','company_rooms',            'full',        'daily',   NULL, TRUE),
        ('company_schema','company_devices',          'full',        'daily',   NULL, TRUE),
        ('company_schema','company_users',            'incremental', 'hourly',  NULL, TRUE),
        ('company_schema','company_smart_plugs',      'incremental', '15min',   NULL, TRUE),
        ('company_schema','company_plug_assignments', 'incremental', '15min',   NULL, TRUE),
        ('company_schema','company_readings',         'incremental', '5min',    NULL, TRUE);

/*  ─────────────────────────────────────────────────────────
    KORIŠTENJE U ETL-u (pseudo-Python):
      cfg = SELECT * FROM metadata.etl_config WHERE active = TRUE;
      for each row:
          if row.load_type == 'incremental':
              run_incremental(row.schema_name, row.table_name)
          else:
              run_full(row.schema_name, row.table_name)
      -- nakon uspjeha → UPDATE metadata.etl_config SET last_loaded_at = now() …
   ───────────────────────────────────────────────────────── */
