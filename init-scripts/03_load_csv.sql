-- 02_load_csv.sql

-- Učitavanje podataka iz CSV-ova u public.* tablice

COPY public.users (id, name, email, created_at,eco_points, updated_at)
FROM '/data/public_users_init.csv'
DELIMITER ','
CSV HEADER;

COPY public.locations (id, name, country, updated_at)
FROM '/data/public_locations_init.csv'
DELIMITER ','
CSV HEADER;

COPY public.rooms (id, name, location_id, updated_at)
FROM '/data/public_rooms_init.csv'
DELIMITER ','
CSV HEADER;

COPY public.devices (id, name, category, created_at, updated_at)
FROM '/data/public_devices_init.csv'
DELIMITER ','
CSV HEADER;

COPY public.smart_plugs (id, user_id, room_id, device_id, status, created_at, updated_at)
FROM '/data/public_smart_plugs_init.csv'
DELIMITER ','
CSV HEADER;

COPY public.plug_assignments (id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at)
FROM '/data/public_plug_assignments_init.csv'
DELIMITER ','
CSV HEADER;

COPY public.readings (id, plug_id, timestamp, power_kwh, created_at, updated_at)
FROM '/data/public_readings_init.csv'
DELIMITER ','
CSV HEADER;

-- Učitavanje podataka iz CSV-ova u company_schema.* tablice

COPY company_schema.companies (id, name, industry, created_at, updated_at)
FROM '/data/company_schema_companies_init.csv'
DELIMITER ','
CSV HEADER;

COPY company_schema.company_locations (id, name, country, co2_factor, company_id, updated_at)
FROM '/data/company_schema_company_locations_init.csv'
DELIMITER ','
CSV HEADER;

COPY company_schema.departments (id, company_id, name, updated_at)
FROM '/data/company_schema_departments_init.csv'
DELIMITER ','
CSV HEADER;

COPY company_schema.company_rooms (id, name, location_id, updated_at)
FROM '/data/company_schema_company_rooms_init.csv'
DELIMITER ','
CSV HEADER;

COPY company_schema.company_devices (id, name, category, created_at, updated_at)
FROM '/data/company_schema_company_devices_init.csv'
DELIMITER ','
CSV HEADER;

COPY company_schema.company_users (id, company_id, name, email, department_id, role, created_at, updated_at)
FROM '/data/company_schema_company_users_init.csv'
DELIMITER ','
CSV HEADER;

COPY company_schema.company_smart_plugs (id, company_id, room_id, device_id, status, created_at, updated_at)
FROM '/data/company_schema_company_smart_plugs_init.csv'
DELIMITER ','
CSV HEADER;

COPY company_schema.company_plug_assignments (id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at)
FROM '/data/company_schema_company_plug_assignments_init.csv'
DELIMITER ','
CSV HEADER;

COPY company_schema.company_readings (id, plug_id, timestamp, power_kwh, created_at, updated_at)
FROM '/data/company_schema_company_readings_init.csv'
DELIMITER ','
CSV HEADER;
