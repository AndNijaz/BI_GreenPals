TRUNCATE landing.devices,
         landing.locations,
         landing.plug_assignments,
         landing.readings,
         landing.rooms,
         landing.smart_plugs,
         landing.users,
         landing.company_devices,
         landing.company_locations,
         landing.company_plug_assignments,
         landing.company_readings,
         landing.company_rooms,
         landing.company_smart_plugs,
         landing.company_users,
         landing.companies,
         landing.departments
RESTART IDENTITY;

-- FULL LOAD: Public Tables
INSERT INTO landing.devices (id, name, category, created_at, updated_at)
SELECT id, name, category, created_at, updated_at FROM public.devices;

INSERT INTO landing.locations (id, name, country)
SELECT id, name, country FROM public.locations;

INSERT INTO landing.plug_assignments (id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at)
SELECT id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at FROM public.plug_assignments;

INSERT INTO landing.readings (id, plug_id, timestamp, power_kwh, created_at, updated_at)
SELECT id, plug_id, timestamp, power_kwh, created_at, updated_at FROM public.readings;

INSERT INTO landing.rooms (id, name, location_id)
SELECT id, name, location_id FROM public.rooms;

INSERT INTO landing.smart_plugs (id, user_id, room_id, device_id, status, created_at, updated_at)
SELECT id, user_id, room_id, device_id, status, created_at, updated_at FROM public.smart_plugs;

INSERT INTO landing.users (id, name, email, eco_points, created_at, updated_at)
SELECT id, name, email, eco_points, created_at, updated_at FROM public.users;

-- FULL LOAD: Company Schema Tables
INSERT INTO landing.company_devices (id, name, category, created_at, updated_at)
SELECT id, name, category, created_at, updated_at FROM company_schema.company_devices;

INSERT INTO landing.company_locations (id, name, country, co2_factor, company_id)
SELECT id, name, country, co2_factor, company_id FROM company_schema.company_locations;

INSERT INTO landing.company_plug_assignments (id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at)
SELECT id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at FROM company_schema.company_plug_assignments;

INSERT INTO landing.company_readings (id, plug_id, timestamp, power_kwh, created_at, updated_at)
SELECT id, plug_id, timestamp, power_kwh, created_at, updated_at FROM company_schema.company_readings;

INSERT INTO landing.company_rooms (id, name, location_id)
SELECT id, name, location_id FROM company_schema.company_rooms;

INSERT INTO landing.company_smart_plugs (id, company_id, room_id, device_id, status, created_at, updated_at)
SELECT id, company_id, room_id, device_id, status, created_at, updated_at FROM company_schema.company_smart_plugs;

INSERT INTO landing.company_users (id, company_id, name, email, department_id, role, created_at, updated_at)
SELECT id, company_id, name, email, department_id, role, created_at, updated_at FROM company_schema.company_users;

INSERT INTO landing.companies (id, name, industry, created_at, updated_at)
SELECT id, name, industry, created_at, updated_at FROM company_schema.companies;

INSERT INTO landing.departments (id, company_id, name)
SELECT id, company_id, name FROM company_schema.departments;




-- PUBLIC tabele
ALTER TABLE landing.devices ADD CONSTRAINT pk_devices PRIMARY KEY (id);
ALTER TABLE landing.locations ADD CONSTRAINT pk_locations PRIMARY KEY (id);
ALTER TABLE landing.plug_assignments ADD CONSTRAINT pk_plug_assignments PRIMARY KEY (id);
ALTER TABLE landing.readings ADD CONSTRAINT pk_readings PRIMARY KEY (id);
ALTER TABLE landing.rooms ADD CONSTRAINT pk_rooms PRIMARY KEY (id);
ALTER TABLE landing.smart_plugs ADD CONSTRAINT pk_smart_plugs PRIMARY KEY (id);
ALTER TABLE landing.users ADD CONSTRAINT pk_users PRIMARY KEY (id);

-- COMPANY tabele
ALTER TABLE landing.company_devices ADD CONSTRAINT pk_company_devices PRIMARY KEY (id);
ALTER TABLE landing.company_locations ADD CONSTRAINT pk_company_locations PRIMARY KEY (id);
ALTER TABLE landing.company_plug_assignments ADD CONSTRAINT pk_company_plug_assignments PRIMARY KEY (id);
ALTER TABLE landing.company_readings ADD CONSTRAINT pk_company_readings PRIMARY KEY (id);
ALTER TABLE landing.company_rooms ADD CONSTRAINT pk_company_rooms PRIMARY KEY (id);
ALTER TABLE landing.company_smart_plugs ADD CONSTRAINT pk_company_smart_plugs PRIMARY KEY (id);
ALTER TABLE landing.company_users ADD CONSTRAINT pk_company_users PRIMARY KEY (id);
ALTER TABLE landing.companies ADD CONSTRAINT pk_companies PRIMARY KEY (id);
ALTER TABLE landing.departments ADD CONSTRAINT pk_departments PRIMARY KEY (id);

-- Složeni ključevi (ako ne koristiš id)
-- Koristi ako ti je logika da readings ima kombinovani ključ
--ALTER TABLE landing.readings ADD CONSTRAINT uq_readings_plug_time UNIQUE (plug_id, timestamp);

-- Ako ti plug_assignments koristi kombinaciju plug_id + start_time kao jedinstvenu
ALTER TABLE landing.plug_assignments ADD CONSTRAINT uq_assignments_plug_start UNIQUE (plug_id, start_time);