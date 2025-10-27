# MonkDB + Kafka + Spark Streaming — Backend Only

This repository contains a ready-to-run backend pipeline for streaming simulated sensor data from Kafka -> PySpark Structured Streaming -> MonkDB HTTP ingest (port 4200).

## Contents
- `.env` - environment variables (edit before use)
- `requirements.txt` - Python dependencies
- `setup/` - helper install scripts (optional)
- `scripts/` - producer, spark consumer, table creation scripts

## Quickstart (after cloning into WSL)
1. Edit `.env` to set your MonkDB credentials and endpoints.
2. Create and activate Python venv:
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```
3. Install Kafka & Zookeeper in WSL (follow Kafka docs or use `setup/install_kafka.sh` if you want it automated).
4. Start Zookeeper and Kafka (in WSL):
```bash
/usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties
/usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties
```
5. Create Kafka topic (once):
```bash
/usr/local/kafka/bin/kafka-topics.sh --create --topic sensor_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
6. Start the producer (in WSL):
```bash
python3 scripts/producer.py
```
7. (Optional) Create table in MonkDB:
```bash
python3 scripts/create_table_monkdb.py
```
8. Run Spark streaming consumer (in WSL):
```bash
python3 scripts/spark_streaming.py
```

## Notes
- This is backend-only (no Streamlit). The scripts assume Kafka is installed under `/usr/local/kafka` in WSL.
- Edit `.env` to match your environment before running.
