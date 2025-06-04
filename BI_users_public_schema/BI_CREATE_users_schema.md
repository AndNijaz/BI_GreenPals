## Users/Public Schema

```
SET search_path TO public;

CREATE TABLE users (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    eco_points INTEGER DEFAULT 0
);

CREATE TABLE locations (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT NOT NULL,
    co2_factor DECIMAL NOT NULL
);

CREATE TABLE rooms (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    location_id INTEGER NOT NULL REFERENCES locations(id) ON DELETE CASCADE
);

CREATE TABLE devices (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL
);

CREATE TABLE smart_plugs (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    room_id INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    device_id INTEGER NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    status BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE plug_assignments (
    id SERIAL PRIMARY KEY,
    plug_id UUID NOT NULL REFERENCES smart_plugs(id) ON DELETE CASCADE,
    room_id INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    device_id INTEGER NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP
);

CREATE TABLE readings (
    id SERIAL PRIMARY KEY,
    plug_id UUID NOT NULL REFERENCES smart_plugs(id) ON DELETE CASCADE,
    timestamp TIMESTAMP NOT NULL,
    power_kwh DECIMAL(6,3) NOT NULL
);

ALTER TABLE locations DROP COLUMN co2_factor;


```

///////////////////

```
CREATE SCHEMA IF NOT EXISTS public;

-- USERS
CREATE TABLE IF NOT EXISTS public.users (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    eco_points INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_users_updated_at
BEFORE UPDATE ON public.users
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- LOCATIONS
CREATE TABLE IF NOT EXISTS public.locations (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT NOT NULL
);

-- ROOMS
CREATE TABLE IF NOT EXISTS public.rooms (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    location_id INTEGER NOT NULL REFERENCES public.locations(id) ON DELETE CASCADE
);

-- DEVICES
CREATE TABLE IF NOT EXISTS public.devices (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_devices_updated_at
BEFORE UPDATE ON public.devices
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- SMART PLUGS
CREATE TABLE IF NOT EXISTS public.smart_plugs (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    room_id INTEGER NOT NULL REFERENCES public.rooms(id) ON DELETE CASCADE,
    device_id INTEGER NOT NULL REFERENCES public.devices(id) ON DELETE CASCADE,
    status BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_smart_plugs_updated_at
BEFORE UPDATE ON public.smart_plugs
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- PLUG ASSIGNMENTS
CREATE TABLE IF NOT EXISTS public.plug_assignments (
    id SERIAL PRIMARY KEY,
    plug_id UUID NOT NULL REFERENCES public.smart_plugs(id) ON DELETE CASCADE,
    room_id INTEGER NOT NULL REFERENCES public.rooms(id) ON DELETE CASCADE,
    device_id INTEGER NOT NULL REFERENCES public.devices(id) ON DELETE CASCADE,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_plug_assignments_updated_at
BEFORE UPDATE ON public.plug_assignments
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- READINGS
CREATE TABLE IF NOT EXISTS public.readings (
    id SERIAL PRIMARY KEY,
    plug_id UUID NOT NULL REFERENCES public.smart_plugs(id) ON DELETE CASCADE,
    timestamp TIMESTAMP NOT NULL,
    power_kwh DECIMAL(6,3) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_readings_updated_at
BEFORE UPDATE ON public.readings
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

```
