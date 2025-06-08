CREATE SCHEMA IF NOT EXISTS public;

-- 1) Definicija funkcije koja će osvježavati updated_at
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2) Kreiranje sheme public (ako već ne postoji)
CREATE SCHEMA IF NOT EXISTS public;

-- 3) Tablica public.users
CREATE TABLE IF NOT EXISTS public.users (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  eco_points INTEGER DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_users_updated_at
BEFORE UPDATE ON public.users
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 4) Tablica public.locations
CREATE TABLE IF NOT EXISTS public.locations (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  country TEXT NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_locations_updated_at
BEFORE UPDATE ON public.locations
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 5) Tablica public.rooms
CREATE TABLE IF NOT EXISTS public.rooms (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  location_id INTEGER NOT NULL REFERENCES public.locations(id) ON DELETE CASCADE,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER trg_rooms_updated_at
BEFORE UPDATE ON public.rooms
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 6) Tablica public.devices
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

-- 7) Tablica public.smart_plugs
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

-- 8) Tablica public.plug_assignments
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

-- 9) Tablica public.readings
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