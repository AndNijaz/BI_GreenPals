## Companies Schema

```
-- Ako šema ne postoji, kreiraj je
CREATE SCHEMA IF NOT EXISTS company_schema;

-- Postavi šemu kao aktivnu za ovu sesiju
SET search_path TO company_schema;

-- 1. Kreiraj kompanije
CREATE TABLE companies (
id UUID PRIMARY KEY,
name TEXT NOT NULL,
industry TEXT NOT NULL,
created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2. Kreiraj lokacije BEZ company_id
CREATE TABLE company_locations (
id SERIAL PRIMARY KEY,
name TEXT NOT NULL,
country TEXT NOT NULL,
co2_factor DECIMAL NOT NULL
);

-- 3. Dodaj kolonu company_id u company_locations i poveži je na companies
ALTER TABLE company_locations
ADD COLUMN company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE;

-- 4. Sada dodaj kolonu location_id u companies i poveži je na company_locations
ALTER TABLE companies
ADD COLUMN location_id INTEGER REFERENCES company_locations(id);

-- 5. Departments
CREATE TABLE departments (
id SERIAL PRIMARY KEY,
company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
name TEXT NOT NULL
);

-- 6. Company users
CREATE TABLE company_users (
id UUID PRIMARY KEY,
company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
name TEXT NOT NULL,
email TEXT NOT NULL UNIQUE,
department_id INTEGER REFERENCES departments(id) ON DELETE SET NULL,
role TEXT NOT NULL
);

-- 7. Rooms
CREATE TABLE company_rooms (
id SERIAL PRIMARY KEY,
name TEXT NOT NULL,
location_id INTEGER NOT NULL REFERENCES company_locations(id) ON DELETE CASCADE
);

-- 8. Devices
CREATE TABLE company_devices (
id SERIAL PRIMARY KEY,
name TEXT NOT NULL,
category TEXT NOT NULL
);

-- 9. Smart plugs
CREATE TABLE company_smart_plugs (
id UUID PRIMARY KEY,
company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
room_id INTEGER NOT NULL REFERENCES company_rooms(id) ON DELETE CASCADE,
device_id INTEGER NOT NULL REFERENCES company_devices(id) ON DELETE CASCADE,
status BOOLEAN NOT NULL DEFAULT TRUE,
created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 10. Plug assignments
CREATE TABLE company_plug_assignments (
id SERIAL PRIMARY KEY,
plug_id UUID NOT NULL REFERENCES company_smart_plugs(id) ON DELETE CASCADE,
room_id INTEGER NOT NULL REFERENCES company_rooms(id) ON DELETE CASCADE,
device_id INTEGER NOT NULL REFERENCES company_devices(id) ON DELETE CASCADE,
start_time TIMESTAMP NOT NULL,
end_time TIMESTAMP
);

-- 11. Readings
CREATE TABLE company_readings (
id SERIAL PRIMARY KEY,
plug_id UUID NOT NULL REFERENCES company_smart_plugs(id) ON DELETE CASCADE,
timestamp TIMESTAMP NOT NULL,
power_kwh DECIMAL(6,3) NOT NULL
);
```
