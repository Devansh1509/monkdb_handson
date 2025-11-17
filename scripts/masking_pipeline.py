# """
# masking_pipeline.py — Apply JS UDF masking → masked table
# """
# from monkdb import client

# MONK_URL = "http://0.0.0.0:4200"
# MONK_USER = "devansh"
# MONK_PASS = "devansh"
# SCHEMA = "etl"

# RAW = f"{SCHEMA}.raw_user_stream_events"
# MASKED = f"{SCHEMA}.raw_user_stream_events_masked"

# def log(msg):
#     print(msg, flush=True)

# def connect():
#     return client.connect(MONK_URL, username=MONK_USER, password=MONK_PASS)

# def main():
#     log("🚀 Masking Pipeline Started...")
#     conn = connect()
#     cur = conn.cursor()

#     # DELETE instead of TRUNCATE
#     log("🧹 Clearing previous masked table (DELETE)...")
#     cur.execute(f"DELETE FROM {MASKED}")
#     conn.commit()

#     # Load RAW table
#     log("📥 Loading RAW events...")
#     cur.execute(f"""
#         SELECT 
#             event_id, event_ts, ingestion_ts,
#             user_id, username, session_id, device_type, app_version,
#             watch_time, country, city, region_code, lat, lon,
#             company_id, company_name, branch_id, branch_name,
#             department, manager_id, manager_name,
#             employee_id, employee_name, project_code, project_name
#         FROM {RAW}
#     """)
#     rows = cur.fetchall()
#     log(f"📦 Loaded {len(rows)} records")

#     # Insert masked rows
#     log("🛡️ Applying masking + inserting into masked table...")
#     inserted = 0

#     for r in rows:
#         (
#             event_id, event_ts, ingestion_ts,
#             user_id, username, session_id, device_type, app_version,
#             watch_time, country, city, region_code,
#             lat, lon, company_id, company_name,
#             branch_id, branch_name, department,
#             manager_id, manager_name,
#             employee_id, employee_name, project_code, project_name
#         ) = r

#         cur.execute(f"""
#             INSERT INTO {MASKED} (
#                 event_id, event_ts, ingestion_ts,
#                 user_id, masked_user_id,
#                 session_id, device_type, app_version,
#                 watch_time, country, city, region_code,
#                 lat, lon, company_id, company_name,
#                 branch_id, branch_name, department,
#                 manager_id, manager_name,
#                 employee_id, employee_name, project_code, project_name
#             )
#             VALUES (
#                 ?, ?, ?, 
#                 ?, etl.mask_email_js(?),
#                 ?, ?, ?,
#                 ?, ?, ?, 
#                 ?, ?, ?, ?, 
#                 ?, ?, ?, 
#                 ?, ?, 
#                 ?, ?, ?, ?
#             )
#         """, (
#             event_id, event_ts, ingestion_ts,
#             user_id, user_id,
#             session_id, device_type, app_version,
#             watch_time, country, city, region_code,
#             lat, lon, company_id, company_name,
#             branch_id, branch_name, department,
#             manager_id, manager_name,
#             employee_id, employee_name, project_code, project_name
#         ))

#         inserted += 1

#     conn.commit()
#     log(f"✅ Masked Insert Complete → {inserted} rows")

#     log("🎉 Masking Pipeline Finished!")

# if __name__ == "__main__":
#     main()
"""
masking_pipeline.py — Mask PII fields in RAW table using MonkDB JS UDF
-----------------------------------------------------------------------
Reads from RAW table → applies JS UDF masking → loads into masked table.
"""

from monkdb import client
import sys

# -------------------------------
# CONFIG
# -------------------------------
MONK_URL = "http://0.0.0.0:4200"
MONK_USER = "devansh"
MONK_PASS = "devansh"
SCHEMA = "etl"

RAW = f"{SCHEMA}.raw_user_stream_events"
MASKED = f"{SCHEMA}.raw_user_stream_events_masked"

# -------------------------------
# Helpers
# -------------------------------
def log(msg):
    print(msg, flush=True)

def connect():
    try:
        conn = client.connect(MONK_URL, username=MONK_USER, password=MONK_PASS)
        log("🔌 Connected to MonkDB\n")
        return conn
    except Exception as e:
        log(f"❌ Connection failed: {e}")
        sys.exit(1)

# -------------------------------
# MAIN MASKING FLOW
# -------------------------------
def main():
    log("🚀 Masking Pipeline Started...")
    conn = connect()
    cur = conn.cursor()

    # ----------------------------------------------------------
    # 1. Clear masked table
    # ----------------------------------------------------------
    log("🧹 Clearing previous masked table (DELETE)...")
    try:
        cur.execute(f"DELETE FROM {MASKED}")
        conn.commit()
    except Exception as e:
        log(f"⚠️ DELETE failed: {e}")
        sys.exit(1)

    # ----------------------------------------------------------
    # 2. Count RAW rows
    # ----------------------------------------------------------
    log("📥 Loading RAW events...")
    cur.execute(f"SELECT COUNT(*) FROM {RAW}")
    raw_count = cur.fetchone()[0]
    log(f"📦 Loaded {raw_count} records")

    if raw_count == 0:
        log("⚠️ No data in RAW table — nothing to mask. Exiting.")
        return

    # ----------------------------------------------------------
    # 3. Insert masked data
    # ----------------------------------------------------------
    log("🛡️ Applying masking + inserting into masked table...")

    insert_sql = f"""
        INSERT INTO {MASKED} (
            event_id, event_ts, ingestion_ts,
            user_id, masked_user_id,
            session_id, device_type, app_version,
            watch_time, country, city, region_code, lat, lon,
            company_id, company_name, branch_id, branch_name,
            department, manager_id, manager_name,
            employee_id, employee_name, project_code, project_name
        )
        SELECT
            event_id,
            event_ts,
            ingestion_ts,
            user_id,
            etl.mask_userid_js(user_id) AS masked_user_id,
            session_id,
            device_type,
            app_version,
            watch_time,
            country,
            city,
            region_code,
            lat,
            lon,
            company_id,
            company_name,
            branch_id,
            branch_name,
            department,
            manager_id,
            manager_name,
            employee_id,
            employee_name,
            project_code,
            project_name
        FROM {RAW};
    """

    try:
        cur.execute(insert_sql)
        conn.commit()
        log(f"✅ Masked Insert Complete → {raw_count} rows")
    except Exception as e:
        log(f"❌ Masked INSERT failed: {e}")
        sys.exit(1)

    # ----------------------------------------------------------
    # DONE
    # ----------------------------------------------------------
    log("🎉 Masking Pipeline Finished!")


if __name__ == "__main__":
    main()
