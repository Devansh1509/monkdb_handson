#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from monkdb import client

load_dotenv()

MONK_HTTP_BASE = os.getenv("MONK_HTTP_BASE", "http://localhost:4200")
MONK_USER = os.getenv("MONK_USER", "devansh")
MONK_PASSWORD = os.getenv("MONK_PASSWORD", "devansh")
MONK_SCHEMA = os.getenv("MONK_SCHEMA", "etl")
MONK_TABLE = os.getenv("MONK_TABLE", "live_stream")

# Parse host and port from MONK_HTTP_BASE if present
base = MONK_HTTP_BASE.replace('http://','').replace('https://','')
parts = base.split(':')
DB_HOST = parts[0]
DB_PORT = parts[1] if len(parts) > 1 else "4200"

print(f"Connecting to MonkDB at {DB_HOST}:{DB_PORT} as {MONK_USER} ...")
conn = client.connect(f"http://{MONK_USER}:{MONK_PASSWORD}@{DB_HOST}:{DB_PORT}", username=MONK_USER)
cur = conn.cursor()
try:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {MONK_SCHEMA}")
except Exception:
    pass
cur.execute(f"CREATE TABLE IF NOT EXISTS {MONK_SCHEMA}.{MONK_TABLE} ("
            ""timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,"
            ""location" TEXT,"
            ""temperature" REAL,"
            ""humidity" REAL,"
            ""wind_speed" REAL)")
print(f"Created (or confirmed) table {MONK_SCHEMA}.{MONK_TABLE}")
cur.close()
conn.close()
