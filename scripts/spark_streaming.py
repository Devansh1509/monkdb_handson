#!/usr/bin/env python3
import os, json, time
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType, DoubleType
import requests

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_stream")
MONK_HTTP_BASE = os.getenv("MONK_HTTP_BASE", "http://localhost:4200")
MONK_HTTP_INGEST = os.getenv("MONK_HTTP_INGEST", "/api/v1/collections/raw_sensors/insert")
MONK_INGEST_URL = MONK_HTTP_BASE.rstrip('/') + MONK_HTTP_INGEST

def post_chunk(records):
    try:
        resp = requests.post(MONK_INGEST_URL, json={'docs': records}, timeout=15)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[ERROR] Failed to post to MonkDB: {e}")
        return False

def process_batch(df, batch_id):
    pdf = df.toPandas()
    if pdf.empty:
        print(f"Batch {batch_id} empty.")
        return
    records = [json.loads(row['value']) for _, row in pdf.iterrows()]
    success = post_chunk(records)
    print(f"Batch {batch_id}: sent {len(records)} records -> {'OK' if success else 'FAIL'}")

def main():
    spark = SparkSession.builder         .appName('KafkaToMonkDB')         .master('local[*]')         .config('spark.sql.shuffle.partitions', '2')         .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    schema = StructType().add('timestamp', StringType()).add('location', StringType())                          .add('temperature', DoubleType()).add('humidity', DoubleType()).add('wind_speed', DoubleType())

    kafka_df = spark.readStream.format('kafka')         .option('kafka.bootstrap.servers', KAFKA_BROKER)         .option('subscribe', KAFKA_TOPIC)         .option('startingOffsets', 'latest')         .load()

    parsed = kafka_df.selectExpr('CAST(value AS STRING) as value')

    query = parsed.writeStream.foreachBatch(process_batch).outputMode('append')             .option('checkpointLocation', './checkpoint_kafka_monkdb')             .start()

    print('Streaming started. Press Ctrl+C to stop.')
    query.awaitTermination()

if __name__ == '__main__':
    main()
