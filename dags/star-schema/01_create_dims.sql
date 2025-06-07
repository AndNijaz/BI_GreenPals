-- 01_create_dims.sql
-- Kreiramo sve dimenzije Type 2 (SCD2) i dim_time

-- === 1) Dimenzija vremena (dim_time) ===
CREATE TABLE IF NOT EXISTS public.dim_time (
    time_id       SERIAL PRIMARY KEY,
    full_date     DATE       NOT NULL,
    year          INTEGER    NOT NULL,
    quarter       INTEGER    NOT NULL,
    month         INTEGER    NOT NULL,
    month_name    TEXT       NOT NULL,
    day           INTEGER    NOT NULL,
    day_of_week   INTEGER    NOT NULL,
    hour          INTEGER    NOT NULL,
    minute        INTEGER    NOT NULL,
    is_weekend    BOOLEAN    NOT NULL,
    created_at    TIMESTAMP  NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- === 2) Dimenzija korisnika (dim_user) ===
CREATE TABLE IF NOT EXISTS public.dim_user (
    user_key      SERIAL PRIMARY KEY,
    user_id       UUID       NOT NULL,
    name          TEXT       NOT NULL,
    email         TEXT       NOT NULL,
    eco_points    INTEGER    NOT NULL,
    start_date    TIMESTAMP  NOT NULL,
    end_date      TIMESTAMP  NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_user_active
    ON public.dim_user (user_id, end_date)
    WHERE end_date = '9999-12-31';

-- === 3) Dimenzija lokacije (dim_location) ===
CREATE TABLE IF NOT EXISTS public.dim_location (
    location_key  SERIAL PRIMARY KEY,
    location_id   INTEGER    NOT NULL,
    name          TEXT       NOT NULL,
    country       TEXT       NOT NULL,
    start_date    TIMESTAMP  NOT NULL,
    end_date      TIMESTAMP  NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_location_active
    ON public.dim_location (location_id, end_date)
    WHERE end_date = '9999-12-31';

-- === 4) Dimenzija soba (dim_room) ===
CREATE TABLE IF NOT EXISTS public.dim_room (
    room_key      SERIAL PRIMARY KEY,
    room_id       INTEGER    NOT NULL,
    name          TEXT       NOT NULL,
    location_key  INTEGER    NOT NULL REFERENCES public.dim_location(location_key),
    start_date    TIMESTAMP  NOT NULL,
    end_date      TIMESTAMP  NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_room_active
    ON public.dim_room (room_id, end_date)
    WHERE end_date = '9999-12-31';

-- === 5) Dimenzija uređaja (dim_device) ===
CREATE TABLE IF NOT EXISTS public.dim_device (
    device_key    SERIAL PRIMARY KEY,
    device_id     INTEGER    NOT NULL,
    name          TEXT       NOT NULL,
    category      TEXT       NOT NULL,
    start_date    TIMESTAMP  NOT NULL,
    end_date      TIMESTAMP  NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_device_active
    ON public.dim_device (device_id, end_date)
    WHERE end_date = '9999-12-31';

-- === 6) Dimenzija kompanije (dim_company) ===
CREATE TABLE IF NOT EXISTS public.dim_company (
    company_key   SERIAL PRIMARY KEY,
    company_id    UUID       NOT NULL,
    name          TEXT       NOT NULL,
    industry      TEXT       NOT NULL,
    start_date    TIMESTAMP  NOT NULL,
    end_date      TIMESTAMP  NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_company_active
    ON public.dim_company (company_id, end_date)
    WHERE end_date = '9999-12-31';

-- === 7) Dimenzija odjela (dim_department) ===
CREATE TABLE IF NOT EXISTS public.dim_department (
    department_key   SERIAL PRIMARY KEY,
    department_id    INTEGER    NOT NULL,
    name             TEXT       NOT NULL,
    company_key      INTEGER    NOT NULL REFERENCES public.dim_company(company_key),
    start_date       TIMESTAMP  NOT NULL,
    end_date         TIMESTAMP  NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_department_active
    ON public.dim_department (department_id, end_date)
    WHERE end_date = '9999-12-31';

-- === 8) Dimenzija pametnog utikača (dim_smart_plug) ===
CREATE TABLE IF NOT EXISTS public.dim_smart_plug (
    plug_key        SERIAL PRIMARY KEY,
    plug_id         UUID       NOT NULL,
    user_key        INTEGER    NOT NULL REFERENCES public.dim_user(user_key),
    room_key        INTEGER    NOT NULL REFERENCES public.dim_room(room_key),
    device_key      INTEGER    NOT NULL REFERENCES public.dim_device(device_key),
    company_key     INTEGER    REFERENCES public.dim_company(company_key),
    department_key  INTEGER    REFERENCES public.dim_department(department_key),
    status          BOOLEAN    NOT NULL,
    start_date      TIMESTAMP  NOT NULL,
    end_date        TIMESTAMP  NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_smart_plug_active
    ON public.dim_smart_plug (plug_id, end_date)
    WHERE end_date = '9999-12-31';
