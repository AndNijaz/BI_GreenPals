-- Insert podataka iz operativnih baza u landing

-- PUBLIC
INSERT INTO landing.devices SELECT * FROM public.devices;
INSERT INTO landing.locations SELECT * FROM public.locations;
INSERT INTO landing.plug_assignments SELECT * FROM public.plug_assignments;
INSERT INTO landing.readings SELECT * FROM public.readings;
INSERT INTO landing.rooms SELECT * FROM public.rooms;
INSERT INTO landing.smart_plugs SELECT * FROM public.smart_plugs;
INSERT INTO landing.users SELECT * FROM public.users;

-- COMPANY
INSERT INTO landing.company_devices SELECT * FROM company_schema.company_devices;
INSERT INTO landing.company_locations SELECT * FROM company_schema.company_locations;
INSERT INTO landing.company_plug_assignments SELECT * FROM company_schema.company_plug_assignments;
INSERT INTO landing.company_readings SELECT * FROM company_schema.company_readings;
INSERT INTO landing.company_rooms SELECT * FROM company_schema.company_rooms;
INSERT INTO landing.company_smart_plugs SELECT * FROM company_schema.company_smart_plugs;
INSERT INTO landing.company_users SELECT * FROM company_schema.company_users;
INSERT INTO landing.companies SELECT * FROM company_schema.companies;
INSERT INTO landing.departments SELECT * FROM company_schema.departments;
