# CREATE LANDING SCHEMA FULL SCRIPT

```sql
-- 1) Kreiraj shemu landing (ako već ne postoji)
CREATE SCHEMA IF NOT EXISTS landing;

-- 2) Public schema tabele u landing
CREATE TABLE IF NOT EXISTS landing.devices          AS TABLE public.devices          WITH NO DATA;
CREATE TABLE IF NOT EXISTS landing.locations        AS TABLE public.locations        WITH NO DATA;
CREATE TABLE IF NOT EXISTS landing.plug_assignments AS TABLE public.plug_assignments WITH NO DATA;
CREATE TABLE IF NOT EXISTS landing.readings         AS TABLE public.readings         WITH NO DATA;
CREATE TABLE IF NOT EXISTS landing.rooms            AS TABLE public.rooms            WITH NO DATA;
CREATE TABLE IF NOT EXISTS landing.smart_plugs      AS TABLE public.smart_plugs      WITH NO DATA;
CREATE TABLE IF NOT EXISTS landing.users            AS TABLE public.users            WITH NO DATA;

-- 3) Company_schema tabele u landing
CREATE TABLE IF NOT EXISTS landing.company_devices          AS TABLE company_schema.company_devices          WITH NO DATA;
CREATE TABLE IF NOT EXISTS landing.company_locations        AS TABLE company_schema.company_locations        WITH NO DATA;
CREATE TABLE IF NOT EXISTS landing.company_plug_assignments AS TABLE company_schema.company_plug_assignments WITH NO DATA;
CREATE TABLE IF NOT EXISTS landing.company_readings         AS TABLE company_schema.company_readings         WITH NO DATA;
CREATE TABLE IF NOT EXISTS landing.company_rooms            AS TABLE company_schema.company_rooms            WITH NO DATA;
CREATE TABLE IF NOT EXISTS landing.company_smart_plugs      AS TABLE company_schema.company_smart_plugs      WITH NO DATA;
CREATE TABLE IF NOT EXISTS landing.company_users            AS TABLE company_schema.company_users            WITH NO DATA;
CREATE TABLE IF NOT EXISTS landing.companies                AS TABLE company_schema.companies                WITH NO DATA;
CREATE TABLE IF NOT EXISTS landing.departments              AS TABLE company_schema.departments              WITH NO DATA;

-- 4) Dodatne BI tabele u landing
--CREATE TABLE IF NOT EXISTS landing.co2_factors       AS TABLE BI_co2_factors.co2_factors       WITH NO DATA;
--CREATE TABLE IF NOT EXISTS landing.electricity_prices AS TABLE BI_co2_factors.electricity_prices WITH NO DATA;
```
