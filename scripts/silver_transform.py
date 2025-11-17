# # # # #!/usr/bin/env python3
# # # # import hashlib
# # # # from monkdb import client
# # # # from datetime import datetime, timezone

# # # # # ------------------------------
# # # # # Config
# # # # # ------------------------------
# # # # MONK_URL = "http://0.0.0.0:4200"
# # # # MONK_USER = "devansh"
# # # # MONK_PASS = "devansh"
# # # # SCHEMA = "etl"

# # # # RAW = f"{SCHEMA}.raw_user_stream_events"
# # # # DIM_USER = f"{SCHEMA}.dim_user_scd2"
# # # # FACT = f"{SCHEMA}.fact_user_watch_activity"
# # # # DIM_TIME = f"{SCHEMA}.dim_time_15min"
# # # # WM = f"{SCHEMA}.etl_watermarks"


# # # # # ------------------------------
# # # # # Helpers
# # # # # ------------------------------
# # # # def h(s: str) -> str:
# # # #     """Hash for generating surrogate keys"""
# # # #     return hashlib.sha1(s.encode()).hexdigest()


# # # # def connect():
# # # #     return client.connect(MONK_URL, username=MONK_USER, password=MONK_PASS)


# # # # # ------------------------------
# # # # # Watermark
# # # # # ------------------------------
# # # # def fetch_last_watermark(cur):
# # # #     cur.execute(f"SELECT last_event_ts FROM {WM} WHERE job_name = ?", ("raw_to_silver",))
# # # #     row = cur.fetchone()
# # # #     return row[0] if row else "1970-01-01 00:00:00"


# # # # def update_watermark(conn, cur, ts):
# # # #     cur.execute(f"DELETE FROM {WM} WHERE job_name = ?", ("raw_to_silver",))
# # # #     cur.execute(f"INSERT INTO {WM} (job_name, last_event_ts) VALUES (?, ?)", ("raw_to_silver", ts))
# # # #     conn.commit()


# # # # # ------------------------------
# # # # # Load new raw data since last run
# # # # # ------------------------------
# # # # def load_new_raw(cur, since_ts):
# # # #     cur.execute(
# # # #         f"""
# # # #         SELECT event_id, event_ts, user_id, username, session_id, device_type, app_version,
# # # #                watch_time, country, city, region_code, lat, lon, ingestion_ts
# # # #         FROM {RAW}
# # # #         WHERE event_ts > ?
# # # #         ORDER BY event_ts ASC
# # # #         """,
# # # #         (since_ts,),
# # # #     )
# # # #     return cur.fetchall()


# # # # # ------------------------------
# # # # # SCD Type 2 upsert into dim_user
# # # # # ------------------------------
# # # # def upsert_dim_user(conn, cur, events):
# # # #     latest = {}
# # # #     for (event_id, event_ts, user_id, username, session_id, device_type, app_version,
# # # #          watch_time, country, city, region_code, lat, lon, ingestion_ts) in events:
# # # #         latest[user_id] = (event_ts, username, device_type, app_version, region_code, city)

# # # #     updates = 0
# # # #     inserts = 0

# # # #     for user_id, (evt_ts, username, device_type, app_version, region_code, city) in latest.items():
# # # #         # check current
# # # #         cur.execute(
# # # #             f"""
# # # #             SELECT user_sk, username, device_type, app_version, region_code, city, effective_from, effective_to, is_current
# # # #             FROM {DIM_USER}
# # # #             WHERE user_id = ? AND is_current = TRUE
# # # #             """,
# # # #             (user_id,),
# # # #         )
# # # #         row = cur.fetchone()

# # # #         changed = True
# # # #         if row:
# # # #             _, u, d, a, r, c, ef, et, ic = row
# # # #             changed = any([u != username, d != device_type, a != app_version, r != region_code, c != city])
# # # #             if changed:
# # # #                 # close previous version
# # # #                 cur.execute(
# # # #                     f"UPDATE {DIM_USER} SET effective_to = ?, is_current = FALSE WHERE user_id = ? AND is_current = TRUE",
# # # #                     (evt_ts, user_id),
# # # #                 )
# # # #                 updates += 1

# # # #         if (row is None) or changed:
# # # #             # insert new version
# # # #             user_sk = h(f"{user_id}|{evt_ts}")
# # # #             cur.execute(
# # # #                 f"""
# # # #                 INSERT INTO {DIM_USER}
# # # #                 (user_sk, user_id, username, device_type, app_version, region_code, city,
# # # #                  effective_from, effective_to, is_current)
# # # #                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
# # # #                 """,
# # # #                 (
# # # #                     user_sk, user_id, username, device_type, app_version, region_code, city,
# # # #                     evt_ts, "9999-12-31 23:59:59", True
# # # #                 )
# # # #             )
# # # #             inserts += 1

# # # #     conn.commit()
# # # #     return inserts, updates


# # # # # ------------------------------
# # # # # Build fact table records
# # # # # ------------------------------
# # # # def time_bucket_to_sk(event_ts):
# # # #     dt = datetime.strptime(event_ts, "%Y-%m-%d %H:%M:%S")
# # # #     minute = dt.minute - (dt.minute % 15)
# # # #     bucket = dt.replace(minute=minute, second=0, microsecond=0)
# # # #     return bucket.strftime("%Y%m%d%H%M")


# # # # def load_facts(conn, cur, events):
# # # #     inserted = 0
# # # #     for (event_id, event_ts, user_id, username, session_id, device_type, app_version,
# # # #          watch_time, country, city, region_code, lat, lon, ingestion_ts) in events:

# # # #         # find correct version of user_dim at event time
# # # #         cur.execute(
# # # #             f"""
# # # #             SELECT user_sk FROM {DIM_USER}
# # # #             WHERE user_id = ? AND effective_from <= ? AND ? < effective_to
# # # #             ORDER BY effective_from DESC LIMIT 1
# # # #             """,
# # # #             (user_id, event_ts, event_ts),
# # # #         )
# # # #         row = cur.fetchone()
# # # #         if not row:
# # # #             continue
# # # #         user_sk = row[0]

# # # #         time_sk = time_bucket_to_sk(event_ts)

# # # #         try:
# # # #             cur.execute(
# # # #                 f"""
# # # #                 INSERT INTO {FACT}
# # # #                 (event_id, event_ts, time_15min_sk, user_sk, region_code, lat, lon,
# # # #                  watch_time, device_type, app_version, ingestion_ts)
# # # #                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
# # # #                 """,
# # # #                 (
# # # #                     event_id, event_ts, time_sk, user_sk, region_code, float(lat), float(lon),
# # # #                     float(watch_time), device_type, app_version, ingestion_ts
# # # #                 )
# # # #             )
# # # #             inserted += 1
# # # #         except:
# # # #             pass

# # # #     conn.commit()
# # # #     return inserted


# # # # # ------------------------------
# # # # # Main execution
# # # # # ------------------------------
# # # # def main():
# # # #     conn = connect()
# # # #     cur = conn.cursor()

# # # #     since = fetch_last_watermark(cur)
# # # #     events = load_new_raw(cur, since)

# # # #     if not events:
# # # #         print("No new data. ✅")
# # # #         return

# # # #     user_inserts, user_updates = upsert_dim_user(conn, cur, events)
# # # #     fact_inserts = load_facts(conn, cur, events)

# # # #     last_ts = events[-1][1]
# # # #     update_watermark(conn, cur, last_ts)

# # # #     print(
# # # #         f"✅ Transform complete | New Dim Inserts: {user_inserts}, Updates: {user_updates}, Fact Inserts: {fact_inserts}, Watermark → {last_ts}"
# # # #     )


# # # # if __name__ == "__main__":
# # # #     main()
# # # # 
# # # #!/usr/bin/env python3
# # # from monkdb import client
# # # from datetime import datetime, timezone
# # # import hashlib

# # # # ------------------------------
# # # # Config
# # # # ------------------------------
# # # MONK_URL = "http://0.0.0.0:4200"
# # # MONK_USER = "devansh"
# # # MONK_PASS = "devansh"
# # # SCHEMA = "etl"

# # # RAW = f"{SCHEMA}.raw_user_stream_events"
# # # DIM_USER = f"{SCHEMA}.dim_user_scd2"
# # # FACT = f"{SCHEMA}.fact_user_watch_activity"
# # # DIM_TIME = f"{SCHEMA}.dim_time_15min"
# # # WM = f"{SCHEMA}.etl_watermarks"


# # # # ------------------------------
# # # # Helpers
# # # # ------------------------------
# # # def h(s: str) -> str:
# # #     return hashlib.sha1(s.encode()).hexdigest()

# # # def connect():
# # #     return client.connect(MONK_URL, username=MONK_USER, password=MONK_PASS)


# # # # ------------------------------
# # # # Timestamp Normalization
# # # # ------------------------------
# # # def normalize_ts(ts):
# # #     # Epoch ms
# # #     if isinstance(ts, (int, float)) and ts > 10**12:
# # #         ts = ts / 1000.0
# # #         return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# # #     # Epoch seconds
# # #     if isinstance(ts, (int, float)):
# # #         return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# # #     # String
# # #     if isinstance(ts, str):
# # #         try:
# # #             return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
# # #         except:
# # #             return datetime.fromisoformat(ts.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")

# # #     # datetime
# # #     return ts.strftime("%Y-%m-%d %H:%M:%S")


# # # # ------------------------------
# # # # Watermark
# # # # ------------------------------
# # # def get_watermark(cur):
# # #     cur.execute(f"SELECT last_event_ts FROM {WM} WHERE job_name = ?", ("raw_to_silver",))
# # #     row = cur.fetchone()
# # #     return row[0] if row else "1970-01-01 00:00:00"

# # # def update_watermark(conn, cur, ts):
# # #     ts_clean = normalize_ts(ts)
# # #     cur.execute(f"DELETE FROM {WM} WHERE job_name = ?", ("raw_to_silver",))
# # #     cur.execute(f"INSERT INTO {WM} (job_name, last_event_ts) VALUES (?,?)", ("raw_to_silver", ts_clean))
# # #     conn.commit()


# # # # ------------------------------
# # # # Time-Bucket → Surrogate Key
# # # # ------------------------------
# # # def time_bucket_to_sk(event_ts):
# # #     ts = normalize_ts(event_ts)
# # #     dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
# # #     minute = dt.minute - (dt.minute % 15)
# # #     bucket = dt.replace(minute=minute, second=0, microsecond=0)
# # #     return bucket.strftime("%Y%m%d%H%M")


# # # # ------------------------------
# # # # Load RAW Rows
# # # # ------------------------------
# # # def load_raw(cur, since_ts):
# # #     since_ts = normalize_ts(since_ts)
# # #     cur.execute(
# # #         f"""
# # #         SELECT event_id, event_ts, user_id, username, session_id, device_type, app_version,
# # #                watch_time, country, city, region_code, lat, lon, ingestion_ts
# # #         FROM {RAW}
# # #         WHERE event_ts > ?
# # #         ORDER BY event_ts ASC
# # #         """,
# # #         (since_ts,)
# # #     )

# # #     rows = cur.fetchall()

# # #     # ✅ Convert user_id to STRING to match DIM_USER type
# # #     normalized = []
# # #     for r in rows:
# # #         r = list(r)
# # #         r[2] = str(r[2])   # user_id index = 2
# # #         normalized.append(tuple(r))

# # #     return normalized


# # # # ------------------------------
# # # # SCD Type-2 Dimension Update
# # # # ------------------------------
# # # # def upsert_users(conn, cur, events):
# # # #     latest = {}
# # # #     for e in events:
# # # #         (_, event_ts, user_id, username, _, device_type, app_version,
# # # #          _, _, city, region_code, _, _, _) = e

# # # #         event_ts = normalize_ts(event_ts)
# # # #         latest[user_id] = (event_ts, username, device_type, app_version, region_code, city)

# # # #     inserts, updates = 0, 0

# # # #     for user_id, (evt_ts, username, device_type, app_version, region_code, city) in latest.items():

# # # #         cur.execute(
# # # #             f"""
# # # #             SELECT user_sk, username, device_type, app_version, region_code, city
# # # #             FROM {DIM_USER}
# # # #             WHERE user_id = ? AND is_current = TRUE
# # # #             """,
# # # #             (user_id,)
# # # #         )
# # # #         row = cur.fetchone()

# # # #         change = True
# # # #         if row:
# # # #             _, u, d, a, r, c = row
# # # #             change = any([u != username, d != device_type, a != app_version, r != region_code, c != city])
# # # #             if change:
# # # #                 cur.execute(
# # # #                     f"UPDATE {DIM_USER} SET effective_to=?, is_current=FALSE WHERE user_id=? AND is_current=TRUE",
# # # #                     (evt_ts, user_id)
# # # #                 )
# # # #                 updates += 1

# # # #         if (row is None) or change:
# # # #             user_sk = h(f"{user_id}|{evt_ts}")
# # # #             # ✅ FIXED: 10 values for 10 columns
# # # #             cur.execute(
# # # #                 f"""
# # # #                 INSERT INTO {DIM_USER}
# # # #                 (user_sk, user_id, username, device_type, app_version,
# # # #                  region_code, city, effective_from, effective_to, is_current)
# # # #                 VALUES (?,?,?,?,?,?,?,?,?,?)
# # # #                 """,
# # # #                 (user_sk, user_id, username, device_type, app_version,
# # # #                  region_code, city, evt_ts, "9999-12-31 23:59:59", True)
# # # #             )
# # # #             inserts += 1

# # # #     conn.commit()
# # # #     return inserts, updates

# # # def upsert_users(conn, cur, events):
# # #     latest = {}
# # #     for e in events:
# # #         (_, event_ts, user_id, username, _, device_type, app_version,
# # #          _, _, city, region_code, _, _, _) = e

# # #         evt_ts = normalize_ts(event_ts)  # ✅ always a clean string
# # #         latest[user_id] = (evt_ts, username, device_type, app_version, region_code, city)

# # #     inserts, updates = 0, 0

# # #     for user_id, (evt_ts, username, device_type, app_version, region_code, city) in latest.items():

# # #         cur.execute(
# # #             f"""
# # #             SELECT user_sk, username, device_type, app_version, region_code, city
# # #             FROM {DIM_USER}
# # #             WHERE user_id = ? AND is_current = TRUE
# # #             """,
# # #             (user_id,)
# # #         )
# # #         row = cur.fetchone()

# # #         change_needed = True
# # #         if row:
# # #             _, u, d, a, r, c = row
# # #             change_needed = any([u != username, d != device_type, a != app_version, r != region_code, c != city])
# # #             if change_needed:
# # #                 cur.execute(
# # #                     f"""
# # #                     UPDATE {DIM_USER}
# # #                     SET effective_to = ?, is_current = FALSE
# # #                     WHERE user_id = ? AND is_current = TRUE
# # #                     """,
# # #                     (evt_ts, user_id)
# # #                 )
# # #                 updates += 1

# # #         if (row is None) or change_needed:
# # #             user_sk = h(f"{user_id}|{evt_ts}")
# # #             cur.execute(
# # #                 f"""
# # #                 INSERT INTO {DIM_USER}
# # #                 (user_sk, user_id, username, device_type, app_version,
# # #                  region_code, city, effective_from, effective_to, is_current)
# # #                 VALUES (?,?,?,?,?,?,?,?,?,?)
# # #                 """,
# # #                 (user_sk, user_id, username, device_type, app_version,
# # #                  region_code, city, evt_ts, "9999-12-31 23:59:59", True)
# # #             )
# # #             inserts += 1

# # #     conn.commit()
# # #     return inserts, updates



# # # # ------------------------------
# # # # Load FACT Table
# # # # ------------------------------
# # # def load_fact(conn, cur, events):
# # #     inserted = 0

# # #     for e in events:
# # #         (event_id, event_ts, user_id, _, _, device_type, app_version,
# # #          watch_time, _, _, region_code, lat, lon, ingestion_ts) = e

# # #         event_ts = normalize_ts(event_ts)

# # #         cur.execute(
# # #             f"""
# # #             SELECT user_sk FROM {DIM_USER}
# # #             WHERE user_id = ?
# # #             AND effective_from <= ?
# # #             AND ? < effective_to
# # #             ORDER BY effective_from DESC LIMIT 1
# # #             """,
# # #             (user_id, event_ts, event_ts)
# # #         )
# # #         row = cur.fetchone()
# # #         if not row:
# # #             continue

# # #         user_sk = row[0]
# # #         time_sk = time_bucket_to_sk(event_ts)

# # #         try:
# # #             cur.execute(
# # #                 f"""
# # #                 INSERT INTO {FACT}
# # #                 (event_id, event_ts, time_15min_sk, user_sk, region_code, lat, lon,
# # #                  watch_time, device_type, app_version, ingestion_ts)
# # #                 VALUES (?,?,?,?,?,?,?,?,?,?,?)
# # #                 """,
# # #                 (event_id, event_ts, time_sk, user_sk, region_code,
# # #                  float(lat), float(lon), float(watch_time),
# # #                  device_type, app_version, ingestion_ts)
# # #             )
# # #             inserted += 1
# # #         except:
# # #             pass

# # #     conn.commit()
# # #     return inserted


# # # # ------------------------------
# # # # Main
# # # # ------------------------------
# # # def main():
# # #     conn = connect()
# # #     cur = conn.cursor()

# # #     since = get_watermark(cur)
# # #     events = load_raw(cur, since)

# # #     if not events:
# # #         print("No new data. ✅")
# # #         return

# # #     dim_ins, dim_upd = upsert_users(conn, cur, events)
# # #     fact_ins = load_fact(conn, cur, events)

# # #     update_watermark(conn, cur, events[-1][1])

# # #     print(
# # #         f"✅ Silver Transform Complete | DIM Inserts: {dim_ins} | DIM Updates: {dim_upd} "
# # #         f"| FACT Inserts: {fact_ins} | Watermark → {normalize_ts(events[-1][1])}"
# # #     )


# # # if __name__ == "__main__":
# # #     main()
# # #!/usr/bin/env python3

# # --Latest code from here --------------------------------------------------------------------
"""
silver_transform.py — MonkDB Silver Layer ETL
----------------------------------------------
Transforms raw user stream events into star schema (dim + fact).
Maintains watermark & metadata for incremental loads.
"""

from monkdb import client
from datetime import datetime, timezone
import hashlib

# -------------------------------
# CONFIG
# -------------------------------
MONK_URL = "http://0.0.0.0:4200"
MONK_USER = "devansh"
MONK_PASS = "devansh"
SCHEMA = "etl"

RAW = f"{SCHEMA}.raw_user_stream_events"
FACT = f"{SCHEMA}.fact_user_activity"
WM = f"{SCHEMA}.etl_watermarks"
META = f"{SCHEMA}.meta_metric"

DIM_COMPANY = f"{SCHEMA}.dim_company"
DIM_BRANCH = f"{SCHEMA}.dim_branch"
DIM_MANAGER = f"{SCHEMA}.dim_manager"
DIM_EMPLOYEE = f"{SCHEMA}.dim_employee"
DIM_PROJECT = f"{SCHEMA}.dim_project"

# -------------------------------
# HELPERS
# -------------------------------
def h(s: str) -> str:
    """Hash surrogate key generator"""
    return hashlib.sha1(s.encode()).hexdigest()

def connect():
    return client.connect(MONK_URL, username=MONK_USER, password=MONK_PASS)

def normalize_ts(ts):
    """Safely convert timestamps to UTC strings (handles all formats)."""
    if ts is None:
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Numeric (epoch ms or s)
    if isinstance(ts, (int, float)):
        # if too large → epoch ms
        if ts > 1e12:
            ts = ts / 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Datetime object
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S")

    # String (ISO or standard)
    if isinstance(ts, str):
        try:
            return datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Fallback
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# -------------------------------
# WATERMARK LOGIC
# -------------------------------
def get_watermark(cur):
    cur.execute(f"SELECT last_run_ts FROM {WM} WHERE pipeline_name = ?", ("live_stream",))
    row = cur.fetchone()
    return row[0] if row else "1970-01-01 00:00:00"

def update_watermark(conn, cur, ts):
    ts = normalize_ts(ts)
    cur.execute(f"DELETE FROM {WM} WHERE pipeline_name = ?", ("live_stream",))
    cur.execute(f"INSERT INTO {WM} (pipeline_name, last_run_ts) VALUES (?, ?)", ("live_stream", ts))
    conn.commit()

# -------------------------------
# LOAD RAW DATA
# -------------------------------
def load_new_raw(cur, since_ts):
    since_ts = normalize_ts(since_ts)
    cur.execute(
        f"""
        SELECT event_id, event_ts, ingestion_ts, user_id, username, session_id, device_type, app_version,
               watch_time, country, city, region_code, lat, lon,
               company_id, company_name, branch_id, branch_name, department,
               manager_id, manager_name, employee_id, employee_name, project_code, project_name
        FROM {RAW}
        WHERE event_ts > ?
        ORDER BY event_ts ASC
        """,
        (since_ts,)
    )
    return cur.fetchall()

# -------------------------------
# DIM TABLE UPSERTS
# -------------------------------
def upsert_dim_company(conn, cur, events):
    companies = {}
    for e in events:
        company_id, company_name = e[14], e[15]
        if company_id and company_id not in companies:
            companies[company_id] = company_name

    for cid, cname in companies.items():
        cur.execute(f"DELETE FROM {DIM_COMPANY} WHERE company_id=?", (cid,))
        cur.execute(f"INSERT INTO {DIM_COMPANY} (company_id, company_name, industry) VALUES (?, ?, ?)", (cid, cname, "General"))
    conn.commit()
    print(f"✅ dim_company: {len(companies)} upserted")

def upsert_dim_branch(conn, cur, events):
    branches = {}
    for e in events:
        branch_id, branch_name, company_id, region_code = e[16], e[17], e[14], e[11]
        if branch_id and branch_id not in branches:
            branches[branch_id] = (branch_name, company_id, region_code)

    for bid, (bname, cid, region) in branches.items():
        cur.execute(f"DELETE FROM {DIM_BRANCH} WHERE branch_id=?", (bid,))
        cur.execute(f"INSERT INTO {DIM_BRANCH} (branch_id, branch_name, company_id, region_code) VALUES (?, ?, ?, ?)",
                    (bid, bname, cid, region))
    conn.commit()
    print(f"✅ dim_branch: {len(branches)} upserted")

def upsert_dim_manager(conn, cur, events):
    managers = {}
    for e in events:
        mid, mname, branch_id, dept = e[19], e[20], e[16], e[18]
        if mid and mid not in managers:
            managers[mid] = (mname, branch_id, dept)

    for mid, (mname, branch, dept) in managers.items():
        cur.execute(f"DELETE FROM {DIM_MANAGER} WHERE manager_id=?", (mid,))
        cur.execute(f"INSERT INTO {DIM_MANAGER} (manager_id, manager_name, branch_id, department) VALUES (?, ?, ?, ?)",
                    (mid, mname, branch, dept))
    conn.commit()
    print(f"✅ dim_manager: {len(managers)} upserted")

def upsert_dim_employee(conn, cur, events):
    employees = {}
    for e in events:
        eid, ename, mid, branch, proj = e[21], e[22], e[19], e[16], e[23]
        if eid and eid not in employees:
            employees[eid] = (ename, mid, branch, proj)

    for eid, (ename, mid, branch, proj) in employees.items():
        cur.execute(f"DELETE FROM {DIM_EMPLOYEE} WHERE employee_id=?", (eid,))
        cur.execute(f"INSERT INTO {DIM_EMPLOYEE} (employee_id, employee_name, manager_id, branch_id, project_code) VALUES (?, ?, ?, ?, ?)",
                    (eid, ename, mid, branch, proj))
    conn.commit()
    print(f"✅ dim_employee: {len(employees)} upserted")

def upsert_dim_project(conn, cur, events):
    projects = {}
    for e in events:
        pid, pname, cid, dept = e[23], e[24], e[14], e[18]
        if pid and pid not in projects:
            projects[pid] = (pname, cid, dept)

    for pid, (pname, cid, dept) in projects.items():
        cur.execute(f"DELETE FROM {DIM_PROJECT} WHERE project_code=?", (pid,))
        cur.execute(f"INSERT INTO {DIM_PROJECT} (project_code, project_name, company_id, department) VALUES (?, ?, ?, ?)",
                    (pid, pname, cid, dept))
    conn.commit()
    print(f"✅ dim_project: {len(projects)} upserted")

# -------------------------------
# FACT TABLE
# -------------------------------
def load_fact(conn, cur, events):
    inserted = 0
    for e in events:
        try:
            (
                event_id, event_ts, ingestion_ts, user_id, username, session_id, device_type, app_version,
                watch_time, country, city, region_code, lat, lon,
                company_id, company_name, branch_id, branch_name, department,
                manager_id, manager_name, employee_id, employee_name, project_code, project_name
            ) = e

            event_ts = normalize_ts(event_ts)
            ingestion_ts = normalize_ts(ingestion_ts)

            cur.execute(f"""
                INSERT INTO {FACT} (
                    event_id, user_id, employee_id, project_code, watch_time,
                    session_id, event_ts, ingestion_ts,
                    company_id, branch_id, manager_id, region_code
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event_id, user_id, employee_id, project_code, float(watch_time or 0.0),
                session_id, event_ts, ingestion_ts,
                company_id, branch_id, manager_id, region_code
            ))
            inserted += 1
        except Exception as ex:
            cur.execute(
                f"INSERT INTO {META} (metric_name, metric_formula, description) VALUES (?,?,?)",
                ("fact_insert_error", "cast(float)", f"⚠️ {event_id} failed: {str(ex)}")
            )
    conn.commit()
    print(f"✅ fact_user_activity: {inserted} inserted")

# -------------------------------
# MAIN
# -------------------------------
def main():
    conn = connect()
    cur = conn.cursor()
    print("🔹 Starting Silver Transform...")

    since = get_watermark(cur)
    new_events = load_new_raw(cur, since)

    if not new_events:
        print("No new data found. ✅")
        return

    upsert_dim_company(conn, cur, new_events)
    upsert_dim_branch(conn, cur, new_events)
    upsert_dim_manager(conn, cur, new_events)
    upsert_dim_employee(conn, cur, new_events)
    upsert_dim_project(conn, cur, new_events)
    load_fact(conn, cur, new_events)

    last_ts = new_events[-1][1]  # event_ts of last record
    update_watermark(conn, cur, last_ts)
    print(f"🎯 Silver Transform Complete | New watermark: {normalize_ts(last_ts)}")

if __name__ == "__main__":
    main()



