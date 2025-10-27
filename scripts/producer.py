#!/usr/bin/env python3
from kafka import KafkaProducer
from faker import Faker
import json, time, random, datetime, os
from dotenv import load_dotenv

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sensor_stream")
producer = KafkaProducer(bootstrap_servers=[BROKER],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

fake = Faker()
print(f"Kafka producer sending to {BROKER} topic={TOPIC} ...")

try:
    while True:
        payload = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "location": fake.city(),
            "temperature": round(random.uniform(10, 40), 2),
            "humidity": round(random.uniform(20, 90), 2),
            "wind_speed": round(random.uniform(0, 30), 2)
        }
        producer.send(TOPIC, value=payload)
        print(f"Sent: {payload}")
        time.sleep(1)
except KeyboardInterrupt:
    print('Producer stopped.')
