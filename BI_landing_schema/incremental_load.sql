-- INCREMENTAL LOAD SCRIPT (updated_at based)
-- ========== PUBLIC ==========
-- Incremental load for devices
INSERT INTO landing.devices (id, name, category, created_at, updated_at)
SELECT id, name, category, created_at, updated_at
FROM public.devices
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '2000-01-01') FROM landing.devices)
ON CONFLICT (id) DO UPDATE SET
name=EXCLUDED.name, category=EXCLUDED.category, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at;

-- Incremental load for plug_assignments
INSERT INTO landing.plug_assignments (id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at)
SELECT id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at
FROM public.plug_assignments
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '2000-01-01') FROM landing.plug_assignments)
ON CONFLICT (id) DO UPDATE SET
plug_id=EXCLUDED.plug_id, room_id=EXCLUDED.room_id, device_id=EXCLUDED.device_id, start_time=EXCLUDED.start_time, end_time=EXCLUDED.end_time, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at;

-- Incremental load for readings
INSERT INTO landing.readings (id, plug_id, timestamp, power_kwh, created_at, updated_at)
SELECT id, plug_id, timestamp, power_kwh, created_at, updated_at
FROM public.readings
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '2000-01-01') FROM landing.readings)
ON CONFLICT (id) DO UPDATE SET
plug_id=EXCLUDED.plug_id, timestamp=EXCLUDED.timestamp, power_kwh=EXCLUDED.power_kwh, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at;

-- Incremental load for smart_plugs
INSERT INTO landing.smart_plugs (id, user_id, room_id, device_id, status, created_at, updated_at)
SELECT id, user_id, room_id, device_id, status, created_at, updated_at
FROM public.smart_plugs
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '2000-01-01') FROM landing.smart_plugs)
ON CONFLICT (id) DO UPDATE SET
user_id=EXCLUDED.user_id, room_id=EXCLUDED.room_id, device_id=EXCLUDED.device_id, status=EXCLUDED.status, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at;

-- Incremental load for users
INSERT INTO landing.users (id, name, email, eco_points, created_at, updated_at)
SELECT id, name, email, eco_points, created_at, updated_at
FROM public.users
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '2000-01-01') FROM landing.users)
ON CONFLICT (id) DO UPDATE SET
name=EXCLUDED.name, email=EXCLUDED.email, eco_points=EXCLUDED.eco_points, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at;

-- Incremental load for companies
INSERT INTO landing.companies (id, name, industry, created_at, updated_at)
SELECT id, name, industry, created_at, updated_at
FROM company_schema.companies
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '2000-01-01') FROM landing.companies)
ON CONFLICT (id) DO UPDATE SET
name=EXCLUDED.name, industry=EXCLUDED.industry, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at;

-- ========== COMPANY SCHEMA ==========
-- Incremental load for company_devices
INSERT INTO landing.company_devices (id, name, category, created_at, updated_at)
SELECT id, name, category, created_at, updated_at
FROM company_schema.company_devices
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '2000-01-01') FROM landing.company_devices)
ON CONFLICT (id) DO UPDATE SET
name=EXCLUDED.name, category=EXCLUDED.category, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at;

-- Incremental load for company_plug_assignments
INSERT INTO landing.company_plug_assignments (id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at)
SELECT id, plug_id, room_id, device_id, start_time, end_time, created_at, updated_at
FROM company_schema.company_plug_assignments
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '2000-01-01') FROM landing.company_plug_assignments)
ON CONFLICT (id) DO UPDATE SET
plug_id=EXCLUDED.plug_id, room_id=EXCLUDED.room_id, device_id=EXCLUDED.device_id, start_time=EXCLUDED.start_time, end_time=EXCLUDED.end_time, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at;

-- Incremental load for company_readings
INSERT INTO landing.company_readings (id, plug_id, timestamp, power_kwh, created_at, updated_at)
SELECT id, plug_id, timestamp, power_kwh, created_at, updated_at
FROM company_schema.company_readings
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '2000-01-01') FROM landing.company_readings)
ON CONFLICT (id) DO UPDATE SET
plug_id=EXCLUDED.plug_id, timestamp=EXCLUDED.timestamp, power_kwh=EXCLUDED.power_kwh, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at;

-- Incremental load for company_smart_plugs
INSERT INTO landing.company_smart_plugs (id, company_id, room_id, device_id, status, created_at, updated_at)
SELECT id, company_id, room_id, device_id, status, created_at, updated_at
FROM company_schema.company_smart_plugs
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '2000-01-01') FROM landing.company_smart_plugs)
ON CONFLICT (id) DO UPDATE SET
company_id=EXCLUDED.company_id, room_id=EXCLUDED.room_id, device_id=EXCLUDED.device_id, status=EXCLUDED.status, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at;

-- Incremental load for company_users
INSERT INTO landing.company_users (id, company_id, name, email, department_id, role, created_at, updated_at)
SELECT id, company_id, name, email, department_id, role, created_at, updated_at
FROM company_schema.company_users
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '2000-01-01') FROM landing.company_users)
ON CONFLICT (id) DO UPDATE SET
company_id=EXCLUDED.company_id, name=EXCLUDED.name, email=EXCLUDED.email, department_id=EXCLUDED.department_id, role=EXCLUDED.role, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at;