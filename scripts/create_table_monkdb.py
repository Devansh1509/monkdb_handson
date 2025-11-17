# # # # from monkdb import client

# # # # # MonkDB connection details
# # # # MONK_HOST = "http://0.0.0.0:4200"
# # # # MONK_USER = "devansh"
# # # # MONK_PASSWORD = "devansh"
# # # # MONK_SCHEMA = "etl"
# # # # MONK_TABLE = "live_stream"

# # # # # Connect to MonkDB
# # # # try:
# # # #     conn = client.connect(f"{MONK_HOST}", username=MONK_USER, password=MONK_PASSWORD)
# # # #     cur = conn.cursor()
# # # #     print("✅ Connected to MonkDB successfully.")
# # # # except Exception as e:
# # # #     print(f"❌ Connection error: {e}")
# # # #     exit(1)

# # # # # Drop table if it exists
# # # # try:
# # # #     cur.execute(f"DROP TABLE IF EXISTS {MONK_SCHEMA}.{MONK_TABLE}")
# # # #     print(f"🗑️ Dropped old table {MONK_SCHEMA}.{MONK_TABLE}")
# # # # except Exception as e:
# # # #     print(f"⚠️ Warning: {e}")

# # # # # Create new table
# # # # # create_query = f"""
# # # # # CREATE TABLE IF NOT EXISTS {MONK_SCHEMA}.{MONK_TABLE} (
# # # # #     "timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
# # # # #     "location" TEXT NOT NULL,
# # # # #     "temperature" REAL NOT NULL,
# # # # #     "humidity" REAL NOT NULL,
# # # # #     "wind_speed" REAL NOT NULL,s
# # # # #     PRIMARY KEY ("timestamp")
# # # # # )
# # # # # """
# # # # create_query = f"""
# # # # CREATE TABLE IF NOT EXISTS {MONK_SCHEMA}.{MONK_TABLE} (
# # # #     id INT NOT NULL,
# # # #     username TEXT NOT NULL,
# # # #     watch_time REAL NOT NULL,
# # # #     country TEXT NOT NULL,
# # # #     timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
# # # #     PRIMARY KEY (id, timestamp)
# # # # )
# # # # """

# # # # try:
# # # #     cur.execute(create_query)
# # # #     print(f"✅ Created table {MONK_SCHEMA}.{MONK_TABLE}")
# # # # except Exception as e:
# # # #     print(f"❌ Error creating table: {e}")

# # # # # Commit and close
# # # # conn.commit()
# # # # cur.close()
# # # # conn.close()
# # # # print("✅ MonkDB setup completed successfully.")
# # # #!/usr/bin/env python3
# # # """
# # # Creates RAW, DIM (SCD2), FACT, and TIME DIM tables for the streaming pipeline.
# # # This version is fully compatible with MonkDB (no DATE type, no INSERT OR REPLACE).
# # # """

# # # from monkdb import client
# # # from datetime import datetime, timedelta, timezone
# # # import sys

# # # # -------------------------------
# # # # Connection
# # # # -------------------------------
# # # MONK_HOST = "http://0.0.0.0:4200"
# # # MONK_USER = "devansh"
# # # MONK_PASSWORD = "devansh"
# # # SCHEMA = "etl"

# # # TABLES = {
# # #     "raw": f"{SCHEMA}.raw_user_stream_events",
# # #     "dim_user": f"{SCHEMA}.dim_user_scd2",
# # #     "dim_region": f"{SCHEMA}.dim_region",
# # #     "dim_time": f"{SCHEMA}.dim_time_15min",
# # #     "fact": f"{SCHEMA}.fact_user_watch_activity",
# # #     "wm": f"{SCHEMA}.etl_watermarks",
# # # }

# # # # -------------------------------
# # # # DDL definitions
# # # # -------------------------------

# # # DDL = {
# # #     "raw": f"""
# # #     CREATE TABLE IF NOT EXISTS {TABLES['raw']} (
# # #         event_id STRING,
# # #         event_ts TIMESTAMP,
# # #         ingestion_ts TIMESTAMP,
# # #         user_id STRING,
# # #         username STRING,
# # #         session_id STRING,
# # #         device_type STRING,
# # #         app_version STRING,
# # #         watch_time DOUBLE,
# # #         country STRING,
# # #         city STRING,
# # #         region_code STRING,
# # #         lat DOUBLE,
# # #         lon DOUBLE,
# # #         PRIMARY KEY (event_id)
# # #     )
# # #     """,

# # #     "dim_user": f"""
# # #     CREATE TABLE IF NOT EXISTS {TABLES['dim_user']} (
# # #         user_sk STRING,
# # #         user_id STRING,
# # #         username STRING,
# # #         device_type STRING,
# # #         app_version STRING,
# # #         region_code STRING,
# # #         city STRING,
# # #         effective_from TIMESTAMP,
# # #         effective_to TIMESTAMP,
# # #         is_current BOOLEAN,
# # #         PRIMARY KEY (user_sk)
# # #     )
# # #     """,

# # #     "dim_region": f"""
# # #     CREATE TABLE IF NOT EXISTS {TABLES['dim_region']} (
# # #         region_code STRING PRIMARY KEY,
# # #         region_name STRING,
# # #         city STRING,
# # #         min_lat DOUBLE,
# # #         max_lat DOUBLE,
# # #         min_lon DOUBLE,
# # #         max_lon DOUBLE
# # #     )
# # #     """,

# # #     # ⬅ FIX: day is STRING instead of DATE
# # #     "dim_time": f"""
# # #     CREATE TABLE IF NOT EXISTS {TABLES['dim_time']} (
# # #         time_15min_sk STRING PRIMARY KEY,
# # #         bucket_start_ts TIMESTAMP,
# # #         day STRING,
# # #         hour INT,
# # #         minute INT,
# # #         dow INT,
# # #         week INT,
# # #         month INT,
# # #         year INT
# # #     )
# # #     """,

# # #     "fact": f"""
# # #     CREATE TABLE IF NOT EXISTS {TABLES['fact']} (
# # #         event_id STRING PRIMARY KEY,
# # #         event_ts TIMESTAMP,
# # #         time_15min_sk STRING,
# # #         user_sk STRING,
# # #         region_code STRING,
# # #         lat DOUBLE,
# # #         lon DOUBLE,
# # #         watch_time DOUBLE,
# # #         device_type STRING,
# # #         app_version STRING,
# # #         ingestion_ts TIMESTAMP
# # #     )
# # #     """,

# # #     "wm": f"""
# # #     CREATE TABLE IF NOT EXISTS {TABLES['wm']} (
# # #         job_name STRING PRIMARY KEY,
# # #         last_event_ts TIMESTAMP
# # #     )
# # #     """,
# # # }

# # # # Region seed data
# # # REGION_DATA = [
# # #     ("NCR-NOIDA", "Noida", "Delhi NCR", 28.50, 28.65, 77.30, 77.45),
# # #     ("NCR-GGN", "Gurgaon", "Delhi NCR", 28.38, 28.53, 76.90, 77.15),
# # #     ("NCR-SAKET", "Saket", "Delhi NCR", 28.50, 28.55, 77.18, 77.25),
# # #     ("NCR-DWARKA", "Dwarka", "Delhi NCR", 28.55, 28.63, 77.02, 77.10),
# # #     ("NCR-CP", "ConnaughtPlace", "Delhi NCR", 28.61, 28.64, 77.20, 77.24),
# # # ]

# # # # -------------------------------
# # # # Main logic
# # # # -------------------------------

# # # def run():
# # #     print("🔌 Connecting to MonkDB...")
# # #     conn = client.connect(MONK_HOST, username=MONK_USER, password=MONK_PASSWORD)
# # #     cur = conn.cursor()

# # #     print("\n🛠 Creating tables...")
# # #     for name, sql in DDL.items():
# # #         cur.execute(sql)
# # #         print(f"✅ {name} table ready.")

# # #     print("\n🌱 Seeding dim_region...")
# # #     for r in REGION_DATA:
# # #         cur.execute(f"DELETE FROM {TABLES['dim_region']} WHERE region_code = ?", (r[0],))
# # #         cur.execute(f"INSERT INTO {TABLES['dim_region']} VALUES (?, ?, ?, ?, ?, ?, ?)", r)
# # #     print("✅ Region dimension seeded.")

# # #     print("\n⏳ Seeding dim_time (15-minute buckets)...")
# # #     now = datetime.now(timezone.utc)
# # #     start = now - timedelta(days=30)
# # #     end = now + timedelta(days=30)

# # #     t = start.replace(minute=(start.minute // 15) * 15, second=0, microsecond=0)
# # #     count = 0

# # #     while t <= end:
# # #         sk = t.strftime("%Y%m%d%H%M")

# # #         # Safe UPSERT pattern for MonkDB: DELETE → INSERT
# # #         cur.execute(f"DELETE FROM {TABLES['dim_time']} WHERE time_15min_sk = ?", (sk,))
# # #         cur.execute(
# # #             f"""
# # #             INSERT INTO {TABLES['dim_time']}
# # #             (time_15min_sk, bucket_start_ts, day, hour, minute, dow, week, month, year)
# # #             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
# # #             """,
# # #             (
# # #                 sk,
# # #                 t.strftime("%Y-%m-%d %H:%M:%S"),
# # #                 t.date().isoformat(),
# # #                 t.hour,
# # #                 t.minute,
# # #                 t.isoweekday(),
# # #                 int(t.strftime("%V")),
# # #                 t.month,
# # #                 t.year,
# # #             )
# # #         )

# # #         t += timedelta(minutes=15)
# # #         count += 1

# # #     print(f"✅ dim_time seeded ({count} rows).")

# # #     print("\n⧗ Initializing ETL watermark...")
# # #     cur.execute(f"DELETE FROM {TABLES['wm']} WHERE job_name = ?", ("raw_to_silver",))
# # #     cur.execute(f"INSERT INTO {TABLES['wm']} VALUES (?, ?)", ("raw_to_silver", "1970-01-01 00:00:00"))

# # #     conn.commit()
# # #     cur.close()
# # #     conn.close()

# # #     print("\n🎉 Schema setup complete.\n")


# # # if __name__ == "__main__":
# # #     run()
# # #!/usr/bin/env python3
# # """
# # create_schema.py
# # ----------------
# # Initializes MonkDB schema for the hierarchical analytics pipeline.
# # Creates RAW, DIM, FACT, META tables and seeds static dimensions and metadata.

# # Compatible with MonkDB’s SQL dialect (no INSERT OR REPLACE).
# # """

# # from monkdb import client
# # from datetime import datetime, timedelta, timezone

# # # -------------------------------
# # # Connection Config
# # # -------------------------------
# # MONK_HOST = "http://0.0.0.0:4200"
# # MONK_USER = "devansh"
# # MONK_PASSWORD = "devansh"
# # SCHEMA = "etl"

# # # -------------------------------
# # # Table Definitions
# # # -------------------------------
# # TABLES = {
# #     "raw": f"{SCHEMA}.raw_user_stream_events",
# #     "dim_user": f"{SCHEMA}.dim_user_scd2",
# #     "dim_region": f"{SCHEMA}.dim_region",
# #     "dim_time": f"{SCHEMA}.dim_time_15min",
# #     "dim_employee": f"{SCHEMA}.dim_employee",
# #     "dim_project": f"{SCHEMA}.dim_project",
# #     "fact": f"{SCHEMA}.fact_user_watch_activity",
# #     "wm": f"{SCHEMA}.etl_watermarks",
# #     "meta_table": f"{SCHEMA}.meta_table",
# #     "meta_column": f"{SCHEMA}.meta_column",
# #     "meta_metric": f"{SCHEMA}.meta_metric",
# # }

# # # -------------------------------
# # # DDL Statements
# # # -------------------------------
# # DDL = {
# #     # RAW layer
# #     "raw": f"""
# #     # CREATE TABLE IF NOT EXISTS {TABLES['raw']} (
# #     #     event_id STRING,
# #     #     event_ts TIMESTAMP,
# #     #     ingestion_ts TIMESTAMP,
# #     #     user_id STRING,
# #     #     username STRING,
# #     #     session_id STRING,
# #     #     device_type STRING,
# #     #     app_version STRING,
# #     #     watch_time DOUBLE,
# #     #     country STRING,
# #     #     city STRING,
# #     #     region_code STRING,
# #     #     lat DOUBLE,
# #     #     lon DOUBLE,
# #     #     company_id STRING,
# #     #     company_name STRING,
# #     #     branch_id STRING,
# #     #     branch_name STRING,
# #     #     department STRING,
# #     #     manager_id STRING,
# #     #     manager_name STRING,
# #     #     employee_id STRING,
# #     #     employee_name STRING,
# #     #     project_code STRING,
# #     #     project_name STRING,
# #     #     PRIMARY KEY (event_id)
# #     # )
# #     CREATE TABLE IF NOT EXISTS etl.raw_user_stream_events (
# #     event_id STRING,
# #     event_ts DATETIME,
# #     ingestion_ts DATETIME,
# #     user_id STRING,
# #     username STRING,
# #     session_id STRING,
# #     device_type STRING,
# #     app_version STRING,
# #     watch_time FLOAT,
# #     country STRING,
# #     city STRING,
# #     region_code STRING,
# #     lat FLOAT,
# #     lon FLOAT,
# #     company_id STRING,
# #     company_name STRING,
# #     branch_id STRING,
# #     branch_name STRING,
# #     department STRING,
# #     manager_id STRING,
# #     manager_name STRING,
# #     employee_id STRING,
# #     employee_name STRING,
# #     project_code STRING,
# #     project_name STRING
# # )

# #     """,

# #     # Region Dimension
# #     "dim_region": f"""
# #     CREATE TABLE IF NOT EXISTS {TABLES['dim_region']} (
# #         region_code STRING PRIMARY KEY,
# #         region_name STRING,
# #         city STRING,
# #         min_lat DOUBLE,
# #         max_lat DOUBLE,
# #         min_lon DOUBLE,
# #         max_lon DOUBLE
# #     )
# #     """,

# #     # Time Dimension
# #     "dim_time": f"""
# #     CREATE TABLE IF NOT EXISTS {TABLES['dim_time']} (
# #         time_15min_sk STRING PRIMARY KEY,
# #         bucket_start_ts TIMESTAMP,
# #         day STRING,
# #         hour INT,
# #         minute INT,
# #         dow INT,
# #         week INT,
# #         month INT,
# #         year INT
# #     )
# #     """,

# #     # Employee Dimension (new)
# #     "dim_employee": f"""
# #     CREATE TABLE IF NOT EXISTS {TABLES['dim_employee']} (
# #         employee_sk STRING PRIMARY KEY,
# #         employee_id STRING,
# #         employee_name STRING,
# #         manager_id STRING,
# #         manager_name STRING,
# #         department STRING,
# #         branch_id STRING,
# #         branch_name STRING,
# #         company_name STRING,
# #         effective_from TIMESTAMP
# #     )
# #     """,

# #     # Project Dimension (new)
# #     "dim_project": f"""
# #     CREATE TABLE IF NOT EXISTS {TABLES['dim_project']} (
# #         project_code STRING PRIMARY KEY,
# #         project_name STRING,
# #         company_name STRING,
# #         created_at TIMESTAMP
# #     )
# #     """,

# #     # FACT table
# #     "fact": f"""
# #     CREATE TABLE IF NOT EXISTS {TABLES['fact']} (
# #         event_id STRING PRIMARY KEY,
# #         event_ts TIMESTAMP,
# #         time_15min_sk STRING,
# #         employee_sk STRING,
# #         project_code STRING,
# #         region_code STRING,
# #         lat DOUBLE,
# #         lon DOUBLE,
# #         watch_time DOUBLE,
# #         device_type STRING,
# #         app_version STRING,
# #         ingestion_ts TIMESTAMP,
# #         manager_id STRING,
# #         branch_id STRING,
# #         department STRING,
# #         company_name STRING
# #     )
# #     """,

# #     # Watermark
# #     "wm": f"""
# #     CREATE TABLE IF NOT EXISTS {TABLES['wm']} (
# #         job_name STRING PRIMARY KEY,
# #         last_event_ts TIMESTAMP
# #     )
# #     """,

# #     # Metadata Tables
# #     "meta_table": f"""
# #     CREATE TABLE IF NOT EXISTS {TABLES['meta_table']} (
# #         table_name STRING PRIMARY KEY,
# #         schema_name STRING,
# #         table_type STRING,
# #         description STRING,
# #         record_count INT,
# #         last_updated TIMESTAMP
# #     )
# #     """,

# #     "meta_column": f"""
# #     CREATE TABLE IF NOT EXISTS {TABLES['meta_column']} (
# #         table_name STRING,
# #         column_name STRING,
# #         data_type STRING,
# #         role STRING,
# #         description STRING
# #     )
# #     """,

# #     "meta_metric": f"""
# #     CREATE TABLE IF NOT EXISTS {TABLES['meta_metric']} (
# #         metric_name STRING PRIMARY KEY,
# #         sql_definition STRING,
# #         business_definition STRING,
# #         category STRING
# #     )
# #     """,
# # }

# # # -------------------------------
# # # Region Data Seed
# # # -------------------------------
# # REGION_DATA = [
# #     ("NCR-NOIDA", "Noida", "Delhi NCR", 28.50, 28.65, 77.30, 77.45),
# #     ("NCR-GGN", "Gurgaon", "Delhi NCR", 28.38, 28.53, 76.90, 77.15),
# #     ("NCR-SAKET", "Saket", "Delhi NCR", 28.50, 28.55, 77.18, 77.25),
# #     ("NCR-DWARKA", "Dwarka", "Delhi NCR", 28.55, 28.63, 77.02, 77.10),
# #     ("NCR-CP", "ConnaughtPlace", "Delhi NCR", 28.61, 28.64, 77.20, 77.24),
# # ]

# # # -------------------------------
# # # Metadata Registration
# # # -------------------------------
# # def register_metadata(cur, table_name, table_type, desc):
# #     cur.execute(f"DELETE FROM {TABLES['meta_table']} WHERE table_name = ?", (table_name,))
# #     cur.execute(
# #         f"""
# #         INSERT INTO {TABLES['meta_table']}
# #         (table_name, schema_name, table_type, description, record_count, last_updated)
# #         VALUES (?, ?, ?, ?, ?, ?)
# #         """,
# #         (
# #             table_name,
# #             SCHEMA,
# #             table_type,
# #             desc,
# #             0,
# #             datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
# #         ),
# #     )

# # # -------------------------------
# # # Main Execution
# # # -------------------------------
# # def run():
# #     print("🔌 Connecting to MonkDB...")
# #     conn = client.connect(MONK_HOST, username=MONK_USER, password=MONK_PASSWORD)
# #     cur = conn.cursor()

# #     print("\n🛠 Creating tables...")
# #     for name, ddl in DDL.items():
# #         cur.execute(ddl)
# #         print(f"✅ {name} table ready.")

# #     # Seed Region Dim
# #     print("\n🌍 Seeding dim_region...")
# #     for r in REGION_DATA:
# #         cur.execute(f"DELETE FROM {TABLES['dim_region']} WHERE region_code = ?", (r[0],))
# #         cur.execute(f"INSERT INTO {TABLES['dim_region']} VALUES (?, ?, ?, ?, ?, ?, ?)", r)
# #     print(f"✅ Seeded {len(REGION_DATA)} regions.")

# #     # Seed Time Dim
# #     print("\n⏳ Seeding dim_time (15-min buckets)...")
# #     now = datetime.now(timezone.utc)
# #     start = now - timedelta(days=15)
# #     end = now + timedelta(days=15)
# #     count = 0

# #     t = start.replace(minute=(start.minute // 15) * 15, second=0, microsecond=0)
# #     while t <= end:
# #         sk = t.strftime("%Y%m%d%H%M")
# #         cur.execute(f"DELETE FROM {TABLES['dim_time']} WHERE time_15min_sk = ?", (sk,))
# #         cur.execute(
# #             f"""
# #             INSERT INTO {TABLES['dim_time']}
# #             (time_15min_sk, bucket_start_ts, day, hour, minute, dow, week, month, year)
# #             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
# #             """,
# #             (
# #                 sk,
# #                 t.strftime("%Y-%m-%d %H:%M:%S"),
# #                 t.date().isoformat(),
# #                 t.hour,
# #                 t.minute,
# #                 t.isoweekday(),
# #                 int(t.strftime("%V")),
# #                 t.month,
# #                 t.year,
# #             ),
# #         )
# #         t += timedelta(minutes=15)
# #         count += 1
# #     print(f"✅ Seeded {count} time buckets.")

# #     # Initialize watermark
# #     print("\n💧 Initializing ETL watermark...")
# #     cur.execute(f"DELETE FROM {TABLES['wm']} WHERE job_name = ?", ("raw_to_silver",))
# #     cur.execute(f"INSERT INTO {TABLES['wm']} VALUES (?, ?)", ("raw_to_silver", "1970-01-01 00:00:00"))
# #     print("✅ Watermark initialized.")

# #     # Register all in metadata
# #     print("\n📚 Registering metadata entries...")
# #     for name in ["raw", "dim_region", "dim_time", "dim_employee", "dim_project", "fact"]:
# #         register_metadata(cur, f"{SCHEMA}.{name}", "dim" if "dim" in name else "fact" if name == "fact" else "raw", f"{name} table")
# #     print("✅ Metadata registered.")

# #     conn.commit()
# #     cur.close()
# #     conn.close()

# #     print("\n🎉 Schema setup complete!")


# # if __name__ == "__main__":
# #     run()
# #!/usr/bin/env python3
# """
# create_schema.py — MonkDB ETL schema setup (MonkDB datatype fix)
# """

# import random
# from datetime import datetime, timedelta
# from monkdb import client

# # ---------------------------------------------
# # MonkDB Connection
# # ---------------------------------------------
# MONK_URL = "http://0.0.0.0:4200"
# USERNAME = "devansh"
# PASSWORD = "devansh"
# SCHEMA = "etl"

# CONN = client.connect(MONK_URL, username=USERNAME, password=PASSWORD)
# CUR = CONN.cursor()

# print("🔌 Connecting to MonkDB...")

# # ---------------------------------------------
# # Utility: drop and recreate
# # ---------------------------------------------
# def drop_and_create(table_name: str, ddl: str):
#     CUR.execute(f"DROP TABLE IF EXISTS {table_name}")
#     CUR.execute(ddl)
#     print(f"✅ {table_name} recreated.")

# # ---------------------------------------------
# # RAW TABLE
# # ---------------------------------------------
# drop_and_create(f"{SCHEMA}.raw_user_stream_events", f"""
# CREATE TABLE {SCHEMA}.raw_user_stream_events (
#     event_id TEXT,
#     event_ts TIMESTAMP,
#     ingestion_ts TIMESTAMP,
#     user_id TEXT,
#     username TEXT,
#     session_id TEXT,
#     device_type TEXT,
#     app_version TEXT,
#     watch_time DOUBLE,
#     country TEXT,
#     city TEXT,
#     region_code TEXT,
#     lat DOUBLE,
#     lon DOUBLE,
#     company_id TEXT,
#     company_name TEXT,
#     branch_id TEXT,
#     branch_name TEXT,
#     department TEXT,
#     manager_id TEXT,
#     manager_name TEXT,
#     employee_id TEXT,
#     employee_name TEXT,
#     project_code TEXT,
#     project_name TEXT
# )
# """)

# # ---------------------------------------------
# # DIM TABLES
# # ---------------------------------------------
# drop_and_create(f"{SCHEMA}.dim_company", f"""
# CREATE TABLE {SCHEMA}.dim_company (
#     company_id TEXT,
#     company_name TEXT,
#     industry TEXT
# )
# """)

# drop_and_create(f"{SCHEMA}.dim_branch", f"""
# CREATE TABLE {SCHEMA}.dim_branch (
#     branch_id TEXT,
#     branch_name TEXT,
#     company_id TEXT,
#     region_code TEXT
# )
# """)

# drop_and_create(f"{SCHEMA}.dim_manager", f"""
# CREATE TABLE {SCHEMA}.dim_manager (
#     manager_id TEXT,
#     manager_name TEXT,
#     branch_id TEXT,
#     department TEXT
# )
# """)

# drop_and_create(f"{SCHEMA}.dim_employee", f"""
# CREATE TABLE {SCHEMA}.dim_employee (
#     employee_id TEXT,
#     employee_name TEXT,
#     manager_id TEXT,
#     branch_id TEXT,
#     project_code TEXT
# )
# """)

# drop_and_create(f"{SCHEMA}.dim_project", f"""
# CREATE TABLE {SCHEMA}.dim_project (
#     project_code TEXT,
#     project_name TEXT,
#     company_id TEXT,
#     department TEXT
# )
# """)

# drop_and_create(f"{SCHEMA}.dim_region", f"""
# CREATE TABLE {SCHEMA}.dim_region (
#     region_code TEXT,
#     region_name TEXT
# )
# """)

# drop_and_create(f"{SCHEMA}.dim_time", f"""
# CREATE TABLE {SCHEMA}.dim_time (
#     time_bucket_start TIMESTAMP,
#     time_bucket_end TIMESTAMP,
#     time_key TEXT
# )
# """)

# # FACT TABLE
# drop_and_create(f"{SCHEMA}.fact_user_activity", f"""
# CREATE TABLE {SCHEMA}.fact_user_activity (
#     event_id TEXT,
#     user_id TEXT,
#     employee_id TEXT,
#     project_code TEXT,
#     watch_time DOUBLE,
#     session_id TEXT,
#     event_ts TIMESTAMP,
#     ingestion_ts TIMESTAMP,
#     company_id TEXT,
#     branch_id TEXT,
#     manager_id TEXT,
#     region_code TEXT
# )
# """)

# # META TABLES
# drop_and_create(f"{SCHEMA}.meta_table", f"""
# CREATE TABLE {SCHEMA}.meta_table (
#     table_name TEXT,
#     type TEXT,
#     description TEXT,
#     created_ts TIMESTAMP,
#     updated_ts TIMESTAMP
# )
# """)

# drop_and_create(f"{SCHEMA}.meta_column", f"""
# CREATE TABLE {SCHEMA}.meta_column (
#     table_name TEXT,
#     column_name TEXT,
#     data_type TEXT,
#     is_nullable TEXT,
#     description TEXT
# )
# """)

# drop_and_create(f"{SCHEMA}.meta_metric", f"""
# CREATE TABLE {SCHEMA}.meta_metric (
#     metric_name TEXT,
#     metric_formula TEXT,
#     description TEXT
# )
# """)

# drop_and_create(f"{SCHEMA}.etl_watermarks", f"""
# CREATE TABLE {SCHEMA}.etl_watermarks (
#     pipeline_name TEXT,
#     last_run_ts TIMESTAMP
# )
# """)

# # ---------------------------------------------
# # SEED DATA
# # ---------------------------------------------
# print("\n🌍 Seeding data...")

# regions = [
#     ("NCR", "North Central Region"),
#     ("WST", "Western Region"),
#     ("EST", "Eastern Region"),
#     ("SOU", "Southern Region"),
#     ("NTH", "Northern Region")
# ]
# for r in regions:
#     CUR.execute(f"INSERT INTO {SCHEMA}.dim_region VALUES (?, ?)", r)
# print(f"✅ Seeded {len(regions)} regions.")

# # start_time = datetime(2025, 1, 1)
# # for i in range(0, 24 * 4 * 30):  # 30 days of 15-min buckets
# #     bucket_start = start_time + timedelta(minutes=15 * i)
# #     bucket_end = bucket_start + timedelta(minutes=15)
# #     key = bucket_start.strftime("%Y%m%d_%H%M")
# #     CUR.execute(f"INSERT INTO {SCHEMA}.dim_time VALUES (?, ?, ?)",
# #                 (bucket_start.strftime("%Y-%m-%d %H:%M:%S"),
# #                  bucket_end.strftime("%Y-%m-%d %H:%M:%S"),
# #                  key))
# # print("✅ Seeded time buckets.")
# print("\n⏳ Seeding dim_time (15-min buckets for 2025)...")

# # Full-year coverage (1 Jan 2025 → 31 Dec 2025)
# start_time = datetime(2025, 1, 1, 0, 0, 0)
# end_time = datetime(2025, 12, 31, 23, 59, 59)

# # Calculate total number of 15-min buckets in the year
# total_minutes = int((end_time - start_time).total_seconds() / 60)
# bucket_count = int(total_minutes / 15)

# for i in range(bucket_count + 1):
#     bucket_start = start_time + timedelta(minutes=15 * i)
#     bucket_end = bucket_start + timedelta(minutes=15)
#     key = bucket_start.strftime("%Y%m%d_%H%M")

#     CUR.execute(f"""
#         INSERT INTO {SCHEMA}.dim_time (time_bucket_start, time_bucket_end, time_key)
#         VALUES (?, ?, ?)
#     """, (
#         bucket_start.strftime("%Y-%m-%d %H:%M:%S"),
#         bucket_end.strftime("%Y-%m-%d %H:%M:%S"),
#         key
#     ))

# print(f"✅ Seeded {bucket_count + 1} time buckets (covering {start_time.date()} → {end_time.date()})")

# companies = [
#     ("C001", "Acme Corp", "Retail"),
#     ("C002", "Globex Ltd", "Finance"),
#     ("C003", "Innotech", "Technology")
# ]
# for c in companies:
#     CUR.execute(f"INSERT INTO {SCHEMA}.dim_company VALUES (?, ?, ?)", c)
# print("✅ Seeded companies.")

# branches = [
#     ("B001", "NCR Branch", "C001", "NCR"),
#     ("B002", "Pune Branch", "C001", "WST"),
#     ("B003", "Kolkata Branch", "C002", "EST"),
#     ("B004", "Bangalore Branch", "C003", "SOU")
# ]
# for b in branches:
#     CUR.execute(f"INSERT INTO {SCHEMA}.dim_branch VALUES (?, ?, ?, ?)", b)
# print("✅ Seeded branches.")

# managers = [
#     ("M001", "Rohit Sharma", "B001", "Sales"),
#     ("M002", "Anjali Mehta", "B002", "Engineering"),
#     ("M003", "Karan Patel", "B003", "Finance"),
#     ("M004", "Sonal Gupta", "B004", "Product")
# ]
# for m in managers:
#     CUR.execute(f"INSERT INTO {SCHEMA}.dim_manager VALUES (?, ?, ?, ?)", m)
# print("✅ Seeded managers.")

# projects = [
#     ("P001", "HyperGrowth", "C001", "Sales"),
#     ("P002", "NeuralOps", "C003", "Engineering"),
#     ("P003", "SafeFinance", "C002", "Finance")
# ]
# for p in projects:
#     CUR.execute(f"INSERT INTO {SCHEMA}.dim_project VALUES (?, ?, ?, ?)", p)
# print("✅ Seeded projects.")

# employees = []
# for i in range(1, 21):
#     eid = f"E{1000+i}"
#     emp = (
#         eid,
#         f"Employee_{i}",
#         random.choice(managers)[0],
#         random.choice(branches)[0],
#         random.choice(projects)[0]
#     )
#     employees.append(emp)
# for e in employees:
#     CUR.execute(f"INSERT INTO {SCHEMA}.dim_employee VALUES (?, ?, ?, ?, ?)", e)
# print("✅ Seeded employees.")

# CUR.execute(f"INSERT INTO {SCHEMA}.etl_watermarks VALUES (?, ?)",
#             ("live_stream", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
# print("✅ Watermark initialized.")

# tables = [
#     ("raw_user_stream_events", "RAW", "Ingested user events"),
#     ("dim_company", "DIM", "Company hierarchy"),
#     ("dim_branch", "DIM", "Branch mapping"),
#     ("dim_manager", "DIM", "Manager dimension"),
#     ("dim_employee", "DIM", "Employee details"),
#     ("dim_project", "DIM", "Project information"),
#     ("dim_region", "DIM", "Region mapping"),
#     ("dim_time", "DIM", "Time dimension"),
#     ("fact_user_activity", "FACT", "Aggregated user activity")
# ]
# for t in tables:
#     CUR.execute(f"INSERT INTO {SCHEMA}.meta_table VALUES (?, ?, ?, ?, ?)",
#                 (f"{SCHEMA}.{t[0]}", t[1], t[2],
#                  datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
#                  datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))

# CONN.commit()
# print("\n🎉 Schema setup complete!")
"""
create_schema.py — MonkDB ETL schema setup (MonkDB datatype fix + masked table)
"""

import random
from datetime import datetime, timedelta
from monkdb import client

# ---------------------------------------------
# MonkDB Connection
# ---------------------------------------------
MONK_URL = "http://0.0.0.0:4200"
USERNAME = "devansh"
PASSWORD = "devansh"
SCHEMA = "etl"

CONN = client.connect(MONK_URL, username=USERNAME, password=PASSWORD)
CUR = CONN.cursor()

print("🔌 Connecting to MonkDB...")

# ---------------------------------------------
# Utility: drop and recreate
# ---------------------------------------------
def drop_and_create(table_name: str, ddl: str):
    CUR.execute(f"DROP TABLE IF EXISTS {table_name}")
    CUR.execute(ddl)
    print(f"✅ {table_name} recreated.")

# ---------------------------------------------
# RAW TABLE
# ---------------------------------------------
drop_and_create(f"{SCHEMA}.raw_user_stream_events", f"""
CREATE TABLE {SCHEMA}.raw_user_stream_events (
    event_id TEXT,
    event_ts TIMESTAMP,
    ingestion_ts TIMESTAMP,
    user_id TEXT,
    username TEXT,
    session_id TEXT,
    device_type TEXT,
    app_version TEXT,
    watch_time DOUBLE,
    country TEXT,
    city TEXT,
    region_code TEXT,
    lat DOUBLE,
    lon DOUBLE,
    company_id TEXT,
    company_name TEXT,
    branch_id TEXT,
    branch_name TEXT,
    department TEXT,
    manager_id TEXT,
    manager_name TEXT,
    employee_id TEXT,
    employee_name TEXT,
    project_code TEXT,
    project_name TEXT
)
""")

# ---------------------------------------------
# MASKED RAW TABLE (NEW)
# ---------------------------------------------
drop_and_create(f"{SCHEMA}.raw_user_stream_events_masked", f"""
CREATE TABLE {SCHEMA}.raw_user_stream_events_masked (
    event_id TEXT,
    event_ts TIMESTAMP,
    ingestion_ts TIMESTAMP,
    user_id TEXT,
    username_masked TEXT,
    session_id TEXT,
    device_type TEXT,
    app_version TEXT,
    watch_time DOUBLE,
    country TEXT,
    city TEXT,
    region_code TEXT,
    lat DOUBLE,
    lon DOUBLE,
    company_id TEXT,
    company_name TEXT,
    branch_id TEXT,
    branch_name TEXT,
    department TEXT,
    manager_id TEXT,
    manager_name TEXT,
    employee_id TEXT,
    employee_name TEXT,
    project_code TEXT,
    project_name TEXT
)
""")

# ---------------------------------------------
# DIM TABLES
# ---------------------------------------------
drop_and_create(f"{SCHEMA}.dim_company", f"""
CREATE TABLE {SCHEMA}.dim_company (
    company_id TEXT,
    company_name TEXT,
    industry TEXT
)
""")

drop_and_create(f"{SCHEMA}.dim_branch", f"""
CREATE TABLE {SCHEMA}.dim_branch (
    branch_id TEXT,
    branch_name TEXT,
    company_id TEXT,
    region_code TEXT
)
""")

drop_and_create(f"{SCHEMA}.dim_manager", f"""
CREATE TABLE {SCHEMA}.dim_manager (
    manager_id TEXT,
    manager_name TEXT,
    branch_id TEXT,
    department TEXT
)
""")

drop_and_create(f"{SCHEMA}.dim_employee", f"""
CREATE TABLE {SCHEMA}.dim_employee (
    employee_id TEXT,
    employee_name TEXT,
    manager_id TEXT,
    branch_id TEXT,
    project_code TEXT
)
""")

drop_and_create(f"{SCHEMA}.dim_project", f"""
CREATE TABLE {SCHEMA}.dim_project (
    project_code TEXT,
    project_name TEXT,
    company_id TEXT,
    department TEXT
)
""")

drop_and_create(f"{SCHEMA}.dim_region", f"""
CREATE TABLE {SCHEMA}.dim_region (
    region_code TEXT,
    region_name TEXT
)
""")

drop_and_create(f"{SCHEMA}.dim_time", f"""
CREATE TABLE {SCHEMA}.dim_time (
    time_bucket_start TIMESTAMP,
    time_bucket_end TIMESTAMP,
    time_key TEXT
)
""")

# ---------------------------------------------
# FACT TABLE
# ---------------------------------------------
drop_and_create(f"{SCHEMA}.fact_user_activity", f"""
CREATE TABLE {SCHEMA}.fact_user_activity (
    event_id TEXT,
    user_id TEXT,
    employee_id TEXT,
    project_code TEXT,
    watch_time DOUBLE,
    session_id TEXT,
    event_ts TIMESTAMP,
    ingestion_ts TIMESTAMP,
    company_id TEXT,
    branch_id TEXT,
    manager_id TEXT,
    region_code TEXT
)
""")

# ---------------------------------------------
# META TABLES
# ---------------------------------------------
drop_and_create(f"{SCHEMA}.meta_table", f"""
CREATE TABLE {SCHEMA}.meta_table (
    table_name TEXT,
    type TEXT,
    description TEXT,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP
)
""")

drop_and_create(f"{SCHEMA}.meta_column", f"""
CREATE TABLE {SCHEMA}.meta_column (
    table_name TEXT,
    column_name TEXT,
    data_type TEXT,
    is_nullable TEXT,
    description TEXT
)
""")

drop_and_create(f"{SCHEMA}.meta_metric", f"""
CREATE TABLE {SCHEMA}.meta_metric (
    metric_name TEXT,
    metric_formula TEXT,
    description TEXT
)
""")

drop_and_create(f"{SCHEMA}.etl_watermarks", f"""
CREATE TABLE {SCHEMA}.etl_watermarks (
    pipeline_name TEXT,
    last_run_ts TIMESTAMP
)
""")

# ---------------------------------------------
# SEED DATA
# ---------------------------------------------
print("\n🌍 Seeding data...")

regions = [
    ("NCR", "North Central Region"),
    ("WST", "Western Region"),
    ("EST", "Eastern Region"),
    ("SOU", "Southern Region"),
    ("NTH", "Northern Region")
]
for r in regions:
    CUR.execute(f"INSERT INTO {SCHEMA}.dim_region VALUES (?, ?)", r)
print(f"✅ Seeded {len(regions)} regions.")


print("\n⏳ Seeding dim_time (15-min buckets for 2025)...")

start_time = datetime(2025, 1, 1, 0, 0, 0)
end_time = datetime(2025, 12, 31, 23, 59, 59)
total_minutes = int((end_time - start_time).total_seconds() / 60)
bucket_count = int(total_minutes / 15)

for i in range(bucket_count + 1):
    bucket_start = start_time + timedelta(minutes=15 * i)
    bucket_end = bucket_start + timedelta(minutes=15)
    key = bucket_start.strftime("%Y%m%d_%H%M")

    CUR.execute(f"""
        INSERT INTO {SCHEMA}.dim_time (time_bucket_start, time_bucket_end, time_key)
        VALUES (?, ?, ?)
    """, (
        bucket_start.strftime("%Y-%m-%d %H:%M:%S"),
        bucket_end.strftime("%Y-%m-%d %H:%M:%S"),
        key
    ))

print(f"✅ Seeded {bucket_count + 1} time buckets (full year 2025)")

companies = [
    ("C001", "Acme Corp", "Retail"),
    ("C002", "Globex Ltd", "Finance"),
    ("C003", "Innotech", "Technology")
]
for c in companies:
    CUR.execute(f"INSERT INTO {SCHEMA}.dim_company VALUES (?, ?, ?)", c)
print("✅ Seeded companies.")

branches = [
    ("B001", "NCR Branch", "C001", "NCR"),
    ("B002", "Pune Branch", "C001", "WST"),
    ("B003", "Kolkata Branch", "C002", "EST"),
    ("B004", "Bangalore Branch", "C003", "SOU")
]
for b in branches:
    CUR.execute(f"INSERT INTO {SCHEMA}.dim_branch VALUES (?, ?, ?, ?)", b)
print("✅ Seeded branches.")

managers = [
    ("M001", "Rohit Sharma", "B001", "Sales"),
    ("M002", "Anjali Mehta", "B002", "Engineering"),
    ("M003", "Karan Patel", "B003", "Finance"),
    ("M004", "Sonal Gupta", "B004", "Product")
]
for m in managers:
    CUR.execute(f"INSERT INTO {SCHEMA}.dim_manager VALUES (?, ?, ?, ?)", m)
print("✅ Seeded managers.")

projects = [
    ("P001", "HyperGrowth", "C001", "Sales"),
    ("P002", "NeuralOps", "C003", "Engineering"),
    ("P003", "SafeFinance", "C002", "Finance")
]
for p in projects:
    CUR.execute(f"INSERT INTO {SCHEMA}.dim_project VALUES (?, ?, ?, ?)", p)
print("✅ Seeded projects.")

employees = []
for i in range(1, 21):
    eid = f"E{1000+i}"
    emp = (
        eid,
        f"Employee_{i}",
        random.choice(managers)[0],
        random.choice(branches)[0],
        random.choice(projects)[0]
    )
    employees.append(emp)
for e in employees:
    CUR.execute(f"INSERT INTO {SCHEMA}.dim_employee VALUES (?, ?, ?, ?, ?)", e)
print("✅ Seeded employees.")

CUR.execute(f"INSERT INTO {SCHEMA}.etl_watermarks VALUES (?, ?)",
            ("live_stream", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
print("✅ Watermark initialized.")

tables = [
    ("raw_user_stream_events", "RAW", "Ingested user events"),
    ("raw_user_stream_events_masked", "RAW", "Masked version of RAW events"),
    ("dim_company", "DIM", "Company hierarchy"),
    ("dim_branch", "DIM", "Branch mapping"),
    ("dim_manager", "DIM", "Manager dimension"),
    ("dim_employee", "DIM", "Employee details"),
    ("dim_project", "DIM", "Project information"),
    ("dim_region", "DIM", "Region mapping"),
    ("dim_time", "DIM", "Time dimension"),
    ("fact_user_activity", "FACT", "Aggregated user activity")
]
for t in tables:
    CUR.execute(f"INSERT INTO {SCHEMA}.meta_table VALUES (?, ?, ?, ?, ?)",
                (f"{SCHEMA}.{t[0]}", t[1], t[2],
                 datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                 datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))

CONN.commit()
print("\n🎉 Schema setup complete (including masked table)!")
