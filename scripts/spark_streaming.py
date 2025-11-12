# # # # # # #!/usr/bin/env python3
# # # # # # import sys
# # # # # # import types

# # # # # # # ✅ Monkeypatch distutils.version.LooseVersion if missing
# # # # # # try:
# # # # # #     from distutils.version import LooseVersion
# # # # # # except ImportError:
# # # # # #     import packaging.version
# # # # # #     sys.modules['distutils'] = types.ModuleType('distutils')
# # # # # #     sys.modules['distutils.version'] = types.ModuleType('distutils.version')
# # # # # #     sys.modules['distutils.version'].LooseVersion = packaging.version.Version

# # # # # # import os, json, time
# # # # # # from dotenv import load_dotenv
# # # # # # from pyspark.sql import SparkSession, functions as F
# # # # # # from pyspark.sql.types import StructType, StringType, DoubleType
# # # # # # import requests

# # # # # # load_dotenv()

# # # # # # KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
# # # # # # KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_stream")
# # # # # # MONK_HTTP_BASE = os.getenv("MONK_HTTP_BASE", "http://localhost:4200")
# # # # # # MONK_HTTP_INGEST = os.getenv("MONK_HTTP_INGEST", "/api/v1/tables/etl.live_stream/insert")
# # # # # # MONK_INGEST_URL = MONK_HTTP_BASE.rstrip('/') + MONK_HTTP_INGEST

# # # # # # def post_chunk(records):
# # # # # #     try:
# # # # # #         resp = requests.post(MONK_INGEST_URL, json={'docs': records}, timeout=15)
# # # # # #         resp.raise_for_status()
# # # # # #         return True
# # # # # #     except Exception as e:
# # # # # #         print(f"[ERROR] Failed to post to MonkDB: {e}")
# # # # # #         return False

# # # # # # def process_batch(df, batch_id):
# # # # # #     df.show(truncate=False)

# # # # # #     pdf = df.toPandas()
# # # # # #     if pdf.empty:
# # # # # #         print(f"Batch {batch_id} empty.")
# # # # # #         return
# # # # # #     records = [json.loads(row['value']) for _, row in pdf.iterrows()]
# # # # # #     success = post_chunk(records)
# # # # # #     print(f"Batch {batch_id}: sent {len(records)} records -> {'OK' if success else 'FAIL'}")

# # # # # # def main():
# # # # # #     # spark = SparkSession.builder         .appName('KafkaToMonkDB')         .master('local[*]')         .config('spark.sql.shuffle.partitions', '2')         .getOrCreate()
# # # # # #     spark = (
# # # # # #     SparkSession.builder
# # # # # #     .appName("KafkaToMonkDB")
# # # # # #     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
# # # # # #     .getOrCreate()
# # # # # # )
# # # # # #     spark.sparkContext.setLogLevel('WARN')

# # # # # #     schema = StructType().add('timestamp', StringType()).add('location', StringType())                          .add('temperature', DoubleType()).add('humidity', DoubleType()).add('wind_speed', DoubleType())

# # # # # #     kafka_df = spark.readStream.format('kafka')         .option('kafka.bootstrap.servers', KAFKA_BROKER)         .option('subscribe', KAFKA_TOPIC)         .option('startingOffsets', 'latest')         .load()

# # # # # #     parsed = kafka_df.selectExpr('CAST(value AS STRING) as value')

# # # # # #     query = parsed.writeStream.foreachBatch(process_batch).outputMode('append')             .option('checkpointLocation', './checkpoint_kafka_monkdb')             .start()

# # # # # #     print('Streaming started. Press Ctrl+C to stop.')
# # # # # #     query.awaitTermination()

# # # # # # if __name__ == '__main__':
# # # # # #     main()
# # # # # #!/usr/bin/env python3
# # # # # import sys
# # # # # import types
# # # # # import os
# # # # # import json
# # # # # from dotenv import load_dotenv
# # # # # from monkdb import client
# # # # # from pyspark.sql import SparkSession
# # # # # from pyspark.sql.types import StringType, DoubleType, StructType

# # # # # # --------------------------------------------------------------------
# # # # # # ✅ Patch distutils.version if missing (fix for Spark envs)
# # # # # # --------------------------------------------------------------------
# # # # # try:
# # # # #     from distutils.version import LooseVersion
# # # # # except ImportError:
# # # # #     import packaging.version
# # # # #     sys.modules['distutils'] = types.ModuleType('distutils')
# # # # #     sys.modules['distutils.version'] = types.ModuleType('distutils.version')
# # # # #     sys.modules['distutils.version'].LooseVersion = packaging.version.Version

# # # # # # --------------------------------------------------------------------
# # # # # # ✅ Load env variables (from .env or system)
# # # # # # --------------------------------------------------------------------
# # # # # load_dotenv()

# # # # # KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
# # # # # KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_stream")

# # # # # DB_HOST = os.getenv("MONK_HOST", "0.0.0.0")
# # # # # DB_PORT = os.getenv("MONK_PORT", "4200")
# # # # # DB_USER = os.getenv("MONK_USER", "devansh")
# # # # # DB_PASSWORD = os.getenv("MONK_PASS", "devansh")
# # # # # DB_SCHEMA = os.getenv("MONK_SCHEMA", "etl")
# # # # # TABLE_NAME = os.getenv("MONK_TABLE", "live_stream")

# # # # # # --------------------------------------------------------------------
# # # # # # ✅ Connect to MonkDB client (once, reused by all batches)
# # # # # # --------------------------------------------------------------------
# # # # # def get_connection():
# # # # #     try:
# # # # #         conn = client.connect(
# # # # #             f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
# # # # #             username=DB_USER
# # # # #         )
# # # # #         print("✅ Connected to MonkDB.")
# # # # #         return conn
# # # # #     except Exception as e:
# # # # #         print(f"❌ Connection to MonkDB failed: {e}")
# # # # #         sys.exit(1)

# # # # # CONN = get_connection()
# # # # # CURSOR = CONN.cursor()

# # # # # # --------------------------------------------------------------------
# # # # # # ✅ Schema for parsing Kafka message values
# # # # # # --------------------------------------------------------------------
# # # # # schema = (
# # # # #     StructType()
# # # # #     .add("timestamp", StringType())
# # # # #     .add("location", StringType())
# # # # #     .add("temperature", DoubleType())
# # # # #     .add("humidity", DoubleType())
# # # # #     .add("wind_speed", DoubleType())
# # # # # )

# # # # # # --------------------------------------------------------------------
# # # # # # ✅ Define what happens per micro-batch
# # # # # # --------------------------------------------------------------------
# # # # # def process_batch(df, batch_id):
# # # # #     if df.isEmpty():
# # # # #         print(f"Batch {batch_id} empty.")
# # # # #         return

# # # # #     pdf = df.toPandas()
# # # # #     records = [json.loads(row['value']) for _, row in pdf.iterrows()]

# # # # #     success_count = 0
# # # # #     for r in records:
# # # # #         try:
# # # # #             CURSOR.execute(
# # # # #                 f"""
# # # # #                 INSERT INTO {DB_SCHEMA}.{TABLE_NAME}
# # # # #                 (timestamp, location, temperature, humidity, wind_speed)
# # # # #                 VALUES (?, ?, ?, ?, ?)
# # # # #                 """,
# # # # #                 (
# # # # #                     r.get("timestamp"),
# # # # #                     r.get("location"),
# # # # #                     r.get("temperature"),
# # # # #                     r.get("humidity"),
# # # # #                     r.get("wind_speed"),
# # # # #                 )
# # # # #             )
# # # # #             success_count += 1
# # # # #         except Exception as e:
# # # # #             print(f"⚠️ Failed to insert record: {e}")
# # # # #             continue

# # # # #     CONN.commit()
# # # # #     print(f"✅ Batch {batch_id}: Inserted {success_count} / {len(records)} records")

# # # # # # --------------------------------------------------------------------
# # # # # # ✅ Start Spark Structured Streaming
# # # # # # --------------------------------------------------------------------
# # # # # def main():
# # # # #     spark = (
# # # # #         SparkSession.builder
# # # # #         .appName("KafkaToMonkDBClientIngest")
# # # # #         .config(
# # # # #             "spark.jars.packages",
# # # # #             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
# # # # #         )
# # # # #         .getOrCreate()
# # # # #     )
# # # # #     spark.sparkContext.setLogLevel("WARN")

# # # # #     kafka_df = (
# # # # #         spark.readStream
# # # # #         .format("kafka")
# # # # #         .option("kafka.bootstrap.servers", KAFKA_BROKER)
# # # # #         .option("subscribe", KAFKA_TOPIC)
# # # # #         .option("startingOffsets", "latest")
# # # # #         .load()
# # # # #     )

# # # # #     parsed = kafka_df.selectExpr("CAST(value AS STRING) as value")

# # # # #     query = (
# # # # #         parsed.writeStream
# # # # #         .foreachBatch(process_batch)
# # # # #         .outputMode("append")
# # # # #         .option("checkpointLocation", "./checkpoint_kafka_monkdb_client")
# # # # #         .start()
# # # # #     )

# # # # #     print("🚀 Streaming started. Press Ctrl+C to stop.")
# # # # #     query.awaitTermination()

# # # # # if __name__ == "__main__":
# # # # #     main()
# # # # #!/usr/bin/env python3
# # # # import sys
# # # # import types
# # # # import os
# # # # import json
# # # # from dotenv import load_dotenv
# # # # from monkdb import client
# # # # from pyspark.sql import SparkSession
# # # # from pyspark.sql.types import StringType, DoubleType, IntegerType, StructType

# # # # # --------------------------------------------------------------------
# # # # # ✅ Patch distutils.version.LooseVersion if missing (Spark 3.5 fix)
# # # # # --------------------------------------------------------------------
# # # # try:
# # # #     from distutils.version import LooseVersion
# # # # except ImportError:
# # # #     import packaging.version
# # # #     sys.modules['distutils'] = types.ModuleType('distutils')
# # # #     sys.modules['distutils.version'] = types.ModuleType('distutils.version')
# # # #     sys.modules['distutils.version'].LooseVersion = packaging.version.Version

# # # # # --------------------------------------------------------------------
# # # # # ✅ Load environment variables
# # # # # --------------------------------------------------------------------
# # # # load_dotenv()

# # # # # KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
# # # # # KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "live_stream")
# # # # KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# # # # KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "live_stream")

# # # # DB_HOST = os.getenv("MONK_HOST", "0.0.0.0")
# # # # DB_PORT = os.getenv("MONK_PORT", "4200")
# # # # DB_USER = os.getenv("MONK_USER", "devansh")
# # # # DB_PASSWORD = os.getenv("MONK_PASS", "devansh")
# # # # DB_SCHEMA = os.getenv("MONK_SCHEMA", "etl")
# # # # TABLE_NAME = os.getenv("MONK_TABLE", "live_stream")

# # # # # --------------------------------------------------------------------
# # # # # ✅ MonkDB connection (global persistent)
# # # # # --------------------------------------------------------------------
# # # # def get_connection():
# # # #     try:
# # # #         conn = client.connect(
# # # #             f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
# # # #             username=DB_USER
# # # #         )
# # # #         print("✅ Connected to MonkDB.")
# # # #         return conn
# # # #     except Exception as e:
# # # #         print(f"❌ MonkDB connection failed: {e}")
# # # #         sys.exit(1)

# # # # CONN = get_connection()
# # # # CURSOR = CONN.cursor()

# # # # # --------------------------------------------------------------------
# # # # # ✅ Schema (must match Kafka producer)
# # # # # --------------------------------------------------------------------
# # # # schema = (
# # # #     StructType()
# # # #     .add("id", IntegerType())
# # # #     .add("username", StringType())
# # # #     .add("watch_time", DoubleType())
# # # #     .add("country", StringType())
# # # #     .add("timestamp", StringType())
# # # # )

# # # # # --------------------------------------------------------------------
# # # # # ✅ Batch processor
# # # # # --------------------------------------------------------------------
# # # # def process_batch(df, batch_id):
# # # #     if df.isEmpty():
# # # #         print(f"Batch {batch_id} empty.")
# # # #         return

# # # #     pdf = df.toPandas()
# # # #     records = [json.loads(row['value']) for _, row in pdf.iterrows()]

# # # #     inserted = 0
# # # #     for rec in records:
# # # #         try:
# # # #             CURSOR.execute(
# # # #                 f"""
# # # #                 INSERT INTO {DB_SCHEMA}.{TABLE_NAME}
# # # #                 (id, username, watch_time, country, timestamp)
# # # #                 VALUES (?, ?, ?, ?, ?)
# # # #                 """,
# # # #                 (
# # # #                     rec.get("id"),
# # # #                     rec.get("username"),
# # # #                     rec.get("watch_time"),
# # # #                     rec.get("country"),
# # # #                     rec.get("timestamp"),
# # # #                 )
# # # #             )
# # # #             inserted += 1
# # # #         except Exception as e:
# # # #             print(f"⚠️ Skipped one record due to: {e}")
# # # #             continue

# # # #     CONN.commit()
# # # #     print(f"✅ Batch {batch_id}: {inserted}/{len(records)} records inserted")

# # # # # --------------------------------------------------------------------
# # # # # ✅ Spark Streaming logic
# # # # # --------------------------------------------------------------------
# # # # def main():
# # # #     spark = (
# # # #         SparkSession.builder
# # # #         .appName("KafkaToMonkDB_LiveStream")
# # # #         .config(
# # # #             "spark.jars.packages",
# # # #             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
# # # #         )
# # # #         .getOrCreate()
# # # #     )
# # # #     spark.sparkContext.setLogLevel("WARN")

# # # #     # kafka_df = (
# # # #     #     spark.readStream
# # # #     #     .format("kafka")
# # # #     #     .option("kafka.bootstrap.servers", KAFKA_BROKER)
# # # #     #     .option("subscribe", KAFKA_TOPIC)
# # # #     #     .option("startingOffsets", "latest")
# # # #     #     .load()
# # # #     # )
# # # #     kafka_df = spark.readStream \
# # # #     .format("kafka") \
# # # #     .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS")) \
# # # #     .option("subscribe", os.getenv("KAFKA_TOPIC")) \
# # # #     .option("startingOffsets", "latest") \
# # # #     .option("failOnDataLoss", "false") \
# # # #     .load()


# # # #     parsed = kafka_df.selectExpr("CAST(value AS STRING) as value")

# # # #     query = (
# # # #         parsed.writeStream
# # # #         .foreachBatch(process_batch)
# # # #         .outputMode("append")
# # # #         .option("checkpointLocation", "./checkpoint_live_stream")
# # # #         .start()
# # # #     )

# # # #     print("🚀 Streaming started. Press Ctrl+C to stop.")
# # # #     query.awaitTermination()

# # # # if __name__ == "__main__":
# # # #     main()

# # # import os, json
# # # from datetime import datetime, timezone
# # # from dotenv import load_dotenv
# # # from monkdb import client
# # # from pyspark.sql import SparkSession

# # # load_dotenv()

# # # RAW_TABLE = f"{os.getenv('MONK_SCHEMA')}.raw_user_stream_events"

# # # CONN = client.connect(f"http://{os.getenv('MONK_HOST')}:{os.getenv('MONK_PORT')}",
# # #                       username=os.getenv("MONK_USER"), password=os.getenv("MONK_PASS"))
# # # CUR = CONN.cursor()

# # # def insert(records):
# # #     for r in records:
# # #         CUR.execute(
# # #             f"INSERT INTO {RAW_TABLE} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
# # #             (
# # #                 r["event_id"], r["event_ts"], datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
# # #                 r["user_id"], r["username"], r["session_id"], r["device_type"], r["app_version"],
# # #                 r["watch_time"], r["country"], r["city"], r["region_code"], r["lat"], r["lon"]
# # #             )
# # #         )
# # #     CONN.commit()

# # # def batch(df, bid):
# # #     pdf = df.toPandas()
# # #     records = [json.loads(x) for x in pdf.value]
# # #     insert(records)
# # #     print(f"✅ Batch {bid}: {len(records)} rows")

# # # spark = SparkSession.builder.appName("RawIngest").config(
# # #     "spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
# # # ).getOrCreate()

# # # stream = spark.readStream.format("kafka").option(
# # #     "kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS")
# # # ).option("subscribe", os.getenv("KAFKA_TOPIC")).load()

# # # parsed = stream.selectExpr("CAST(value AS STRING) as value")

# # # parsed.writeStream.foreachBatch(batch).start().awaitTermination()
# # #!/usr/bin/env python3
# # import os, json
# # from datetime import datetime
# # from dotenv import load_dotenv
# # from monkdb import client
# # from pyspark.sql import SparkSession

# # load_dotenv()

# # KAFKA_BOOT = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
# # KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'live_stream')
# # MONK_HOST = os.getenv('MONK_HOST', '0.0.0.0')
# # MONK_PORT = os.getenv('MONK_PORT', '4200')
# # MONK_USER = os.getenv('MONK_USER', 'devansh')
# # MONK_PASS = os.getenv('MONK_PASS', 'devansh')
# # MONK_SCHEMA = os.getenv('MONK_SCHEMA', 'etl')
# # RAW_TABLE = f"{MONK_SCHEMA}.raw_user_stream_events"
# # META_TABLE = f"{MONK_SCHEMA}.meta_table"

# # CONN = client.connect(f"http://{MONK_HOST}:{MONK_PORT}", username=MONK_USER, password=MONK_PASS)
# # CUR = CONN.cursor()

# # def update_metadata(batch_size):
# #     CUR.execute(f"""
# #         INSERT OR REPLACE INTO {META_TABLE}
# #         VALUES (?, ?, ?, ?, ?, ?)
# #     """, (
# #         RAW_TABLE, MONK_SCHEMA, "raw",
# #         "Raw Kafka ingestion events",
# #         batch_size,
# #         datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
# #     ))
# #     CONN.commit()

# # def insert_batch(records):
# #     if not records:
# #         return
# #     for r in records:
# #         CUR.execute(f"""
# #             INSERT INTO {RAW_TABLE} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
# #         """, (
# #             r.get('event_id'), r.get('event_ts'), datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
# #             r.get('user_id'), r.get('username'), r.get('session_id'),
# #             r.get('device_type'), r.get('app_version'), r.get('watch_time'),
# #             r.get('country'), r.get('city'), r.get('region_code'),
# #             r.get('lat'), r.get('lon'),
# #             r.get('company_id'), r.get('company_name'),
# #             r.get('branch_id'), r.get('branch_name'),
# #             r.get('department'), r.get('manager_id'), r.get('manager_name'),
# #             r.get('employee_id'), r.get('employee_name'),
# #             r.get('project_code'), r.get('project_name')
# #         ))
# #     CONN.commit()
# #     update_metadata(len(records))

# # def process_batch(df, batch_id):
# #     if df.isEmpty():
# #         return
# #     pdf = df.toPandas()
# #     records = [json.loads(v) for v in pdf.value]
# #     insert_batch(records)
# #     print(f"✅ Batch {batch_id}: inserted {len(records)} records")

# # spark = SparkSession.builder.appName('KafkaToMonkDB_Raw') \
# #     .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
# #     .getOrCreate()

# # spark.sparkContext.setLogLevel('WARN')

# # df = spark.readStream.format('kafka') \
# #     .option('kafka.bootstrap.servers', KAFKA_BOOT) \
# #     .option('subscribe', KAFKA_TOPIC) \
# #     .option('startingOffsets', 'latest') \
# #     .option('failOnDataLoss', 'false') \
# #     .load()

# # parsed = df.selectExpr("CAST(value AS STRING) as value")

# # query = parsed.writeStream.foreachBatch(process_batch).outputMode('append') \
# #     .option('checkpointLocation', './checkpoint_live_stream').start()

# # print("🚀 Streaming started...")
# # query.awaitTermination()
# #!/usr/bin/env python3
# """
# spark_streaming.py
# ------------------
# Reads Kafka messages → parses JSON events → writes them to MonkDB RAW table.
# Fully aligned with 25-column org-hierarchical schema and includes auto Kafka JAR config.
# """

# import os
# import json
# from datetime import datetime, timezone
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json
# from pyspark.sql.types import (
#     StructType, StructField, StringType, DoubleType
# )
# from monkdb import client

# # -------------------------------
# # MonkDB Config
# # -------------------------------
# MONK_URL = os.getenv("MONK_URL", "http://0.0.0.0:4200")
# MONK_USER = os.getenv("MONK_USER", "devansh")
# MONK_PASS = os.getenv("MONK_PASS", "devansh")
# SCHEMA = os.getenv("MONK_SCHEMA", "etl")
# RAW_TABLE = f"{SCHEMA}.raw_user_stream_events"

# # -------------------------------
# # Kafka Config
# # -------------------------------
# KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# TOPIC = os.getenv("KAFKA_TOPIC", "live_stream")
# CHECKPOINT_DIR = "./checkpoint_live_stream"

# # -------------------------------
# # MonkDB Connection
# # -------------------------------
# CONN = client.connect(MONK_URL, username=MONK_USER, password=MONK_PASS)
# CUR = CONN.cursor()

# # -------------------------------
# # Define schema for incoming JSON
# # -------------------------------
# event_schema = StructType([
#     StructField("event_id", StringType()),
#     StructField("event_ts", StringType()),
#     StructField("user_id", StringType()),
#     StructField("username", StringType()),
#     StructField("session_id", StringType()),
#     StructField("device_type", StringType()),
#     StructField("app_version", StringType()),
#     StructField("watch_time", DoubleType()),
#     StructField("country", StringType()),
#     StructField("city", StringType()),
#     StructField("region_code", StringType()),
#     StructField("lat", DoubleType()),
#     StructField("lon", DoubleType()),
#     StructField("company_id", StringType()),
#     StructField("company_name", StringType()),
#     StructField("branch_id", StringType()),
#     StructField("branch_name", StringType()),
#     StructField("department", StringType()),
#     StructField("manager_id", StringType()),
#     StructField("manager_name", StringType()),
#     StructField("employee_id", StringType()),
#     StructField("employee_name", StringType()),
#     StructField("project_code", StringType()),
#     StructField("project_name", StringType())
# ])

# # -------------------------------
# # Spark Session with Kafka JAR
# # -------------------------------
# spark = (
#     SparkSession.builder
#     .appName("KafkaToMonkDB")
#     .config(
#         "spark.jars.packages",
#         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
#     )
#     .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)
#     .getOrCreate()
# )

# spark.sparkContext.setLogLevel("WARN")

# print("🚀 Streaming started (with Kafka connector loaded)...")

# # -------------------------------
# # Read from Kafka
# # -------------------------------
# try:
#     df = (
#         spark.readStream
#         .format("kafka")
#         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
#         .option("subscribe", TOPIC)
#         .option("startingOffsets", "latest")
#         .load()
#     )
# except Exception as e:
#     print("❌ Kafka source not available.")
#     print("💡 Hint: Spark requires the Kafka connector JAR.")
#     print("🧩 Fix: add .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1')")
#     raise e

# # Convert Kafka binary payload → structured JSON
# df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
#               .select(from_json(col("json_str"), event_schema).alias("data")) \
#               .select("data.*")

# # -------------------------------
# # MonkDB Insert Function
# # -------------------------------
# def insert_batch(records):
#     """Insert microbatch of events into MonkDB RAW table."""
#     if not records:
#         return

#     for r in records:
#         try:
#             CUR.execute(
#                 f"""
#                 INSERT INTO {RAW_TABLE} (
#                     event_id, event_ts, ingestion_ts, user_id, username, session_id, device_type,
#                     app_version, watch_time, country, city, region_code, lat, lon,
#                     company_id, company_name, branch_id, branch_name, department,
#                     manager_id, manager_name, employee_id, employee_name, project_code, project_name
#                 )
#                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
#                 """,
#                 (
#                     r.get('event_id'),
#                     r.get('event_ts'),
#                     datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
#                     r.get('user_id'),
#                     r.get('username'),
#                     r.get('session_id'),
#                     r.get('device_type'),
#                     r.get('app_version'),
#                     float(r.get('watch_time', 0.0)),
#                     r.get('country'),
#                     r.get('city'),
#                     r.get('region_code'),
#                     float(r.get('lat', 0.0)),
#                     float(r.get('lon', 0.0)),
#                     r.get('company_id'),
#                     r.get('company_name'),
#                     r.get('branch_id'),
#                     r.get('branch_name'),
#                     r.get('department'),
#                     r.get('manager_id'),
#                     r.get('manager_name'),
#                     r.get('employee_id'),
#                     r.get('employee_name'),
#                     r.get('project_code'),
#                     r.get('project_name')
#                 )
#             )
#         except Exception as e:
#             print(f"⚠️ Failed to insert event {r.get('event_id')}: {e}")
#             continue

#     CONN.commit()
#     print(f"✅ Inserted batch of {len(records)} records into RAW.")


# def process_batch(df, batch_id):
#     """Callback for Spark foreachBatch."""
#     rows = [row.asDict() for row in df.collect()]
#     insert_batch(rows)


# # -------------------------------
# # Start the Streaming Query
# # -------------------------------
# query = (
#     df_parsed.writeStream
#     .foreachBatch(process_batch)
#     .outputMode("append")
#     .option("checkpointLocation", CHECKPOINT_DIR)
#     .trigger(processingTime="10 seconds")
#     .start()
# )

# query.awaitTermination()

#!/usr/bin/env python3
"""
spark_streaming.py — Stream data from Kafka → MonkDB RAW layer
--------------------------------------------------------------
Handles:
 - Safe ingestion of producer data into MonkDB
 - Automatic replacement of None → 0.0 for numeric fields
 - Metadata tracking for skipped/bad records
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from datetime import datetime
from monkdb import client
import json

# ----------------------------------------
# MonkDB Connection
# ----------------------------------------
MONK_URL = "http://0.0.0.0:4200"
MONK_USER = "devansh"
MONK_PASS = "devansh"
SCHEMA = "etl"
RAW_TABLE = f"{SCHEMA}.raw_user_stream_events"
META_TABLE = f"{SCHEMA}.meta_metric"

CONN = client.connect(MONK_URL, username=MONK_USER, password=MONK_PASS)
CUR = CONN.cursor()

print("🚀 Starting Spark Stream...")

# ----------------------------------------
# Spark Session
# ----------------------------------------
spark = (
    SparkSession.builder
    .appName("KafkaToMonkDB_ETL")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    )
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------------------------
# Kafka Stream Source
# ----------------------------------------
KAFKA_BROKER = "localhost:9092"
TOPIC = "live_stream"

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# ----------------------------------------
# Schema Definition
# ----------------------------------------
schema_cols = [
    "event_id", "event_ts", "user_id", "username", "session_id", "device_type",
    "app_version", "watch_time", "country", "city", "region_code", "lat", "lon",
    "company_id", "company_name", "branch_id", "branch_name", "department",
    "manager_id", "manager_name", "employee_id", "employee_name",
    "project_code", "project_name"
]

# ----------------------------------------
# Utility — Safe Float Conversion
# ----------------------------------------
def safe_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default

# ----------------------------------------
# Insert Batch → MonkDB
# ----------------------------------------
def insert_batch(records):
    inserted, skipped = 0, 0
    for r in records:
        try:
            CUR.execute(f"""
                INSERT INTO {RAW_TABLE} (
                    event_id, event_ts, ingestion_ts,
                    user_id, username, session_id, device_type, app_version,
                    watch_time, country, city, region_code, lat, lon,
                    company_id, company_name, branch_id, branch_name,
                    department, manager_id, manager_name,
                    employee_id, employee_name, project_code, project_name
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                r.get('event_id'),
                r.get('event_ts'),
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                r.get('user_id'),
                r.get('username'),
                r.get('session_id'),
                r.get('device_type'),
                r.get('app_version'),
                safe_float(r.get('watch_time')),
                r.get('country'),
                r.get('city'),
                r.get('region_code'),
                safe_float(r.get('lat')),
                safe_float(r.get('lon')),
                r.get('company_id'),
                r.get('company_name'),
                r.get('branch_id'),
                r.get('branch_name'),
                r.get('department'),
                r.get('manager_id'),
                r.get('manager_name'),
                r.get('employee_id'),
                r.get('employee_name'),
                r.get('project_code'),
                r.get('project_name')
            ))
            inserted += 1

        except Exception as e:
            skipped += 1
            CUR.execute(f"""
                INSERT INTO {META_TABLE} (metric_name, metric_formula, description)
                VALUES (?,?,?)
            """, (
                "stream_insert_error",
                "safe_float(None) → default",
                f"⚠️ Failed to insert event {r.get('event_id')} | Error: {str(e)}"
            ))
            CONN.commit()

    CONN.commit()
    print(f"✅ Inserted {inserted} records | ⚠️ Skipped {skipped}")

# ----------------------------------------
# Batch Processing Function
# ----------------------------------------
def process_batch(df, batch_id):
    records = [
        json.loads(row.value.decode("utf-8"))
        for row in df.collect()
    ]
    if records:
        insert_batch(records)

# ----------------------------------------
# Streaming Query
# ----------------------------------------
query = (
    df.writeStream
    .foreachBatch(process_batch)
    .outputMode("update")
    .option("checkpointLocation", "/tmp/checkpoint_monkdb")
    .start()
)

print("✅ Streaming started. Waiting for data from Kafka...")
query.awaitTermination()
