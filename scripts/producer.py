# # # # import json
# # # # import time
# # # # import random
# # # # from confluent_kafka import Producer

# # # # # Kafka configuration
# # # # conf = {
# # # #     'bootstrap.servers': 'localhost:9092'
# # # # }

# # # # producer = Producer(conf)
# # # # topic = "live_stream"

# # # # def delivery_report(err, msg):
# # # #     if err is not None:
# # # #         print(f"Delivery failed for record {msg.key()}: {err}")
# # # #     else:
# # # #         print(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# # # # # Mock live stream data producer
# # # # while True:
# # # #     data = {
# # # #         "id": random.randint(1000, 9999),
# # # #         "username": random.choice(["user1", "user2", "user3"]),
# # # #         "watch_time": round(random.uniform(5.0, 120.0), 2),
# # # #         "country": random.choice(["IN", "US", "UK", "DE"]),
# # # #         "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
# # # #     }

# # # #     producer.produce(topic, key=str(data["id"]), value=json.dumps(data), callback=delivery_report)
# # # #     producer.poll(0)
# # # #     time.sleep(2)
# # # #!/usr/bin/env python3
# # # import json
# # # import time
# # # import random
# # # import signal
# # # import sys
# # # from confluent_kafka import Producer

# # # # --------------------------------------------------------------------
# # # # ✅ Kafka configuration
# # # # --------------------------------------------------------------------
# # # conf = {
# # #     "bootstrap.servers": "localhost:9092",
# # #     "client.id": "live_stream_producer",
# # #     "acks": "all",
# # # }
# # # producer = Producer(conf)
# # # topic = "live_stream"

# # # # --------------------------------------------------------------------
# # # # ✅ Graceful shutdown
# # # # --------------------------------------------------------------------
# # # running = True
# # # def handle_exit(sig, frame):
# # #     global running
# # #     print("\n🛑 Stopping producer, flushing remaining messages...")
# # #     running = False
# # #     producer.flush(5)
# # #     sys.exit(0)

# # # signal.signal(signal.SIGINT, handle_exit)
# # # signal.signal(signal.SIGTERM, handle_exit)

# # # # --------------------------------------------------------------------
# # # # ✅ Delivery report callback
# # # # --------------------------------------------------------------------
# # # def delivery_report(err, msg):
# # #     if err is not None:
# # #         print(f"❌ Delivery failed for record {msg.key()}: {err}")
# # #     else:
# # #         print(f"✅ Sent → {msg.topic()}[{msg.partition()}] offset={msg.offset()} key={msg.key()}")

# # # # --------------------------------------------------------------------
# # # # ✅ Mock Live Stream Data Producer Loop
# # # # --------------------------------------------------------------------
# # # print("🚀 Live Stream Producer started. Sending messages to Kafka topic: 'live_stream' ...")
# # # print("Press Ctrl+C to stop.\n")

# # # while running:
# # #     try:
# # #         data = {
# # #             "id": random.randint(1000, 9999),
# # #             "username": random.choice(["user1", "user2", "user3"]),
# # #             "watch_time": round(random.uniform(5.0, 120.0), 2),
# # #             "country": random.choice(["IN", "US", "UK", "DE"]),
# # #             "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
# # #         }

# # #         producer.produce(
# # #             topic,
# # #             key=str(data["id"]),
# # #             value=json.dumps(data),
# # #             callback=delivery_report
# # #         )

# # #         # Poll events and flush periodically
# # #         producer.poll(0)
# # #         time.sleep(1.5)

# # #     except BufferError:
# # #         print("⚠️ Buffer full, flushing...")
# # #         producer.flush()
# # #     except Exception as e:
# # #         print(f"⚠️ Unexpected error: {e}")
# # #         time.sleep(2)

# # # # --------------------------------------------------------------------
# # # # ✅ Final flush on exit
# # # # --------------------------------------------------------------------
# # # producer.flush()
# # # print("✅ Producer stopped gracefully.")
# # import json, time, random, signal, sys, uuid
# # from datetime import datetime, timezone
# # from confluent_kafka import Producer

# # conf = {"bootstrap.servers": "localhost:9092", "acks": "all"}
# # producer = Producer(conf)
# # topic = "live_stream"

# # REGIONS = {
# #     "NCR-NOIDA": (28.50, 28.65, 77.30, 77.45),
# #     "NCR-GGN": (28.38, 28.53, 76.90, 77.15),
# #     "NCR-SAKET": (28.50, 28.55, 77.18, 77.25),
# #     "NCR-DWARKA": (28.55, 28.63, 77.02, 77.10),
# #     "NCR-CP": (28.61, 28.64, 77.20, 77.24),
# # }
# # CITIES = {r: n for r, n, *_ in [
# #     ("NCR-NOIDA", "Noida"), ("NCR-GGN", "Gurgaon"),
# #     ("NCR-SAKET", "Saket"), ("NCR-DWARKA", "Dwarka"),
# #     ("NCR-CP", "New Delhi")
# # ]}

# # USER_POOL = [f"U{i}" for i in range(10000, 10500)]
# # DEVICES = ["Android", "iOS", "Web"]
# # APP_VERSIONS = ["1.20", "1.21", "1.22", "2.0"]

# # def stop(sig, frame):
# #     print("Stopping producer...")
# #     producer.flush()
# #     sys.exit(0)

# # signal.signal(signal.SIGINT, stop)

# # sleep_time = 1/150  # ~150 events/sec

# # while True:
# #     region = random.choice(list(REGIONS.keys()))
# #     min_lat, max_lat, min_lon, max_lon = REGIONS[region]

# #     evt = {
# #         "event_id": str(uuid.uuid4()),
# #         "event_ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
# #         "user_id": random.choice(USER_POOL),
# #         "username": "user",
# #         "session_id": f"S{random.randint(1,999999)}",
# #         "device_type": random.choice(DEVICES),
# #         "app_version": random.choice(APP_VERSIONS),
# #         "watch_time": round(random.uniform(3,600),2),
# #         "country": "IN",
# #         "city": CITIES[region],
# #         "region_code": region,
# #         "lat": round(random.uniform(min_lat, max_lat),6),
# #         "lon": round(random.uniform(min_lon, max_lon),6),
# #     }

# #     producer.produce(topic, value=json.dumps(evt))
# #     producer.poll(0)
# #     time.sleep(sleep_time)
# #!/usr/bin/env python3
# import json, time, random, signal, sys, uuid
# from datetime import datetime, timezone
# from confluent_kafka import Producer

# # -------------------------
# # Kafka Config
# # -------------------------
# conf = {
#     "bootstrap.servers": "localhost:9092",
#     "acks": "all",
#     "client.id": "user_stream_producer"
# }

# producer = Producer(conf)
# topic = "live_stream"

# # -------------------------
# # Region + User Simulation Data
# # -------------------------
# REGIONS = {
#     "NCR-NOIDA": (28.50, 28.65, 77.30, 77.45),
#     "NCR-GGN": (28.38, 28.53, 76.90, 77.15),
#     "NCR-SAKET": (28.50, 28.55, 77.18, 77.25),
#     "NCR-DWARKA": (28.55, 28.63, 77.02, 77.10),
#     "NCR-CP": (28.61, 28.64, 77.20, 77.24),
# }

# CITIES = {
#     "NCR-NOIDA": "Noida",
#     "NCR-GGN": "Gurgaon",
#     "NCR-SAKET": "Saket",
#     "NCR-DWARKA": "Dwarka",
#     "NCR-CP": "New Delhi",
# }

# USER_POOL = [f"U{i}" for i in range(10000, 10500)]
# DEVICES = ["Android", "iOS", "Web"]
# APP_VERSIONS = ["1.20", "1.21", "1.22", "2.0"]


# # -------------------------
# # Graceful Shutdown
# # -------------------------
# def handle_exit(sig, frame):
#     print("\n🛑 Stopping producer... Flushing messages...")
#     producer.flush()
#     sys.exit(0)

# signal.signal(signal.SIGINT, handle_exit)
# signal.signal(signal.SIGTERM, handle_exit)


# # -------------------------
# # Delivery Callback
# # -------------------------
# def delivery_report(err, msg):
#     if err is not None:
#         print(f"❌ Delivery failed: {err}")
#     else:
#         print(f"✅ Sent → topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


# print("\n🚀 Producer started (Delhi NCR, ~150 events/sec)")
# print("   Logs will show every event generated.\n")

# sleep_time = 1 / 150.0   # ~150 msgs per second

# # -------------------------
# # Emit Events Continuously
# # -------------------------
# while True:
#     try:
#         region = random.choice(list(REGIONS.keys()))
#         min_lat, max_lat, min_lon, max_lon = REGIONS[region]

#         evt = {
#             "event_id": str(uuid.uuid4()),
#             "event_ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
#             "user_id": random.choice(USER_POOL),
#             "username": "user",
#             "session_id": f"S{random.randint(1,999999)}",
#             "device_type": random.choice(DEVICES),
#             "app_version": random.choice(APP_VERSIONS),
#             "watch_time": round(random.uniform(3,600), 2),
#             "country": "IN",
#             "city": CITIES[region],
#             "region_code": region,
#             "lat": round(random.uniform(min_lat, max_lat), 6),
#             "lon": round(random.uniform(min_lon, max_lon), 6),
#         }

#         producer.produce(topic, value=json.dumps(evt), callback=delivery_report)
#         producer.poll(0)  # serve delivery callbacks

#         # 🎯 NEW: Print message content
#         print(f"→ Produced event: {evt['event_id']} | user={evt['user_id']} | region={evt['region_code']} | watch_time={evt['watch_time']}")

#         time.sleep(sleep_time)

#     except BufferError:
#         print("⚠️ Buffer full, flushing...")
#         producer.flush()
#     except Exception as e:
#         print("⚠️ Unexpected error:", e)
#         time.sleep(0.2)

#!/usr/bin/env python3
import os, json, time, random, signal, sys, uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

# -------------------------------
# Kafka Config
# -------------------------------
BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'live_stream')
RATE = float(os.getenv('EVENTS_PER_SEC', '150'))
LOG_EVERY = int(os.getenv('LOG_EVERY', '50'))

conf = {
    'bootstrap.servers': BOOTSTRAP,
    'acks': 'all',
    'client.id': 'org_stream_producer'
}
producer = Producer(conf)

# -------------------------------
# Synthetic Hierarchy (Moderate)
# -------------------------------
COMPANIES = [("C1", "AcmeCorp"), ("C2", "BlueStone"), ("C3", "HarborTech")]
BRANCHES = {
    'C1': [('B1', 'Noida'), ('B2', 'Gurgaon')],
    'C2': [('B3', 'Mumbai'), ('B4', 'Pune')],
    'C3': [('B5', 'Bengaluru')]
}
DEPARTMENTS = ['Sales', 'Engineering', 'Support', 'Operations']

MANAGERS = {}
EMPLOYEES = {}
PROJECTS = {}

for cid, cname in COMPANIES:
    MANAGERS[cid] = [(f"M_{cid}_{i}", f"Manager_{cid}_{i}") for i in range(1, 5)]
    EMPLOYEES[cid] = [(f"E_{cid}_{i}", f"Employee_{cid}_{i}") for i in range(1, 51)]
    PROJECTS[cid] = [(f"P_{cid}_{i}", f"Project_{cid}_{i}") for i in range(1, 9)]

REGIONS = {
    "NCR-NOIDA": (28.50, 28.65, 77.30, 77.45),
    "NCR-GGN": (28.38, 28.53, 76.90, 77.15),
    "NCR-SAKET": (28.50, 28.55, 77.18, 77.25),
    "NCR-DWARKA": (28.55, 28.63, 77.02, 77.10),
    "NCR-CP": (28.61, 28.64, 77.20, 77.24),
}
CITIES = {k: k.split('-')[-1] for k in REGIONS}
USER_POOL = [f"U{i}" for i in range(20000, 20500)]
DEVICES = ['Android', 'iOS', 'Web']
APP_VERSIONS = ['1.20', '1.21', '1.22', '2.0']

# -------------------------------
# Graceful Shutdown
# -------------------------------
running = True
def handle_exit(sig, frame):
    global running
    print("\n🛑 Stopping producer, flushing...")
    running = False
    producer.flush()
    sys.exit(0)

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

# -------------------------------
# Delivery Callback
# -------------------------------
def delivery_report(err, msg):
    if err:
        print("❌ Delivery failed:", err)

print(f"🚀 Producer started | topic={TOPIC} | rate={RATE}/s\n")
sleep_time = 1.0 / RATE
count = 0

# -------------------------------
# Emit events continuously
# -------------------------------
while running:
    try:
        cid, cname = random.choice(COMPANIES)
        branch_id, branch_name = random.choice(BRANCHES[cid])
        dept = random.choice(DEPARTMENTS)
        manager_id, manager_name = random.choice(MANAGERS[cid])
        employee_id, employee_name = random.choice(EMPLOYEES[cid])
        project_code, project_name = random.choice(PROJECTS[cid])
        region = random.choice(list(REGIONS.keys()))
        min_lat, max_lat, min_lon, max_lon = REGIONS[region]

        evt = {
            "event_id": str(uuid.uuid4()),
            "event_ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "user_id": random.choice(USER_POOL),
            "username": employee_name,
            "session_id": f"S{random.randint(1, 999999)}",
            "device_type": random.choice(DEVICES),
            "app_version": random.choice(APP_VERSIONS),
            "watch_time": round(random.uniform(1, 900), 2),
            "country": "IN",
            "city": CITIES[region],
            "region_code": region,
            "lat": round(random.uniform(min_lat, max_lat), 6),
            "lon": round(random.uniform(min_lon, max_lon), 6),
            "company_id": cid,
            "company_name": cname,
            "branch_id": branch_id,
            "branch_name": branch_name,
            "department": dept,
            "manager_id": manager_id,
            "manager_name": manager_name,
            "employee_id": employee_id,
            "employee_name": employee_name,
            "project_code": project_code,
            "project_name": project_name
        }

        producer.produce(TOPIC, value=json.dumps(evt), callback=delivery_report)
        producer.poll(0)
        count += 1

        if count % LOG_EVERY == 0:
            print(f"✅ Produced {count} | {evt['company_id']}-{evt['branch_id']} → {evt['project_code']}")

        time.sleep(sleep_time)

    except BufferError:
        producer.flush()
    except Exception as e:
        print("⚠️ Producer error:", e)
        time.sleep(0.1)

producer.flush()
