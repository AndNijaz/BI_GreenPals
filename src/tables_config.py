# ----------------------------
#  tables_config.py
#  → centralna definicija meta-podataka za operacione tabele
# ----------------------------

TABLES_PUBLIC = {
    "users": {
        "schema": "public",
        "landing": "landing.users",
        "columns": [
            "id", "name", "email", "eco_points", "created_at", "updated_at"
        ]
    },
    "locations": {
        "schema": "public",
        "landing": "landing.locations",
        "columns": [
            "id", "name", "country", "updated_at"
        ]
    },
    "rooms": {
        "schema": "public",
        "landing": "landing.rooms",
        "columns": [
            "id", "name", "location_id", "updated_at"
        ]
    },
    "devices": {
        "schema": "public",
        "landing": "landing.devices",
        "columns": [
            "id", "name", "category", "created_at", "updated_at"
        ]
    },
    "smart_plugs": {
        "schema": "public",
        "landing": "landing.smart_plugs",
        "columns": [
            "id", "user_id", "room_id", "device_id", "status",
            "created_at", "updated_at"
        ]
    },
    "plug_assignments": {
        "schema": "public",
        "landing": "landing.plug_assignments",
        "columns": [
            "id", "plug_id", "room_id", "device_id",
            "start_time", "end_time", "created_at", "updated_at"
        ]
    },
    "readings": {
        "schema": "public",
        "landing": "landing.readings",
        "columns": [
            "id", "plug_id", "timestamp", "power_kwh",
            "created_at", "updated_at"
        ]
    }
}

TABLES_COMPANY = {
    "companies": {
        "schema": "company_schema",
        "landing": "landing.companies",
        "columns": [
            "id", "name", "industry", "created_at", "updated_at"
        ]
    },
    "company_locations": {
        "schema": "company_schema",
        "landing": "landing.company_locations",
        "columns": [
            "id", "name", "country", "co2_factor", "company_id", "updated_at"
        ]
    },
    "departments": {
        "schema": "company_schema",
        "landing": "landing.departments",
        "columns": [
            "id", "company_id", "name", "updated_at"
        ]
    },
    "company_rooms": {
        "schema": "company_schema",
        "landing": "landing.company_rooms",
        "columns": [
            "id", "name", "location_id", "updated_at"
        ]
    },
    "company_devices": {
        "schema": "company_schema",
        "landing": "landing.company_devices",
        "columns": [
            "id", "name", "category", "created_at", "updated_at"
        ]
    },
    "company_users": {
        "schema": "company_schema",
        "landing": "landing.company_users",
        "columns": [
            "id", "company_id", "name", "email",
            "department_id", "role", "created_at", "updated_at"
        ]
    },
    "company_smart_plugs": {
        "schema": "company_schema",
        "landing": "landing.company_smart_plugs",
        "columns": [
            "id", "company_id", "room_id", "device_id",
            "status", "created_at", "updated_at"
        ]
    },
    "company_plug_assignments": {
        "schema": "company_schema",
        "landing": "landing.company_plug_assignments",
        "columns": [
            "id", "plug_id", "room_id", "device_id",
            "start_time", "end_time", "created_at", "updated_at"
        ]
    },
    "company_readings": {
        "schema": "company_schema",
        "landing": "landing.company_readings",
        "columns": [
            "id", "plug_id", "timestamp", "power_kwh",
            "created_at", "updated_at"
        ]
    }
}
