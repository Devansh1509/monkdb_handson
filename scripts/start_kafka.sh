#!/bin/bash
# Start Zookeeper and Kafka (WSL) assuming Kafka is installed at /usr/local/kafka
echo "Starting Zookeeper..."
/usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties &
sleep 5
echo "Starting Kafka broker..."
/usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &
sleep 5
echo "Creating topic sensor_stream if not exists..."
/usr/local/kafka/bin/kafka-topics.sh --create --topic sensor_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 || true
echo "Kafka should be up."
