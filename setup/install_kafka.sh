#!/bin/bash
# Helper to download & extract Kafka in /usr/local/kafka (WSL).
# Run in WSL as a user with sudo privileges.
set -e
KAFKA_VER="3.7.1"
SCALA_VER="2.13"
DEST="/usr/local/kafka"
echo "Downloading Kafka..."
wget https://downloads.apache.org/kafka/${KAFKA_VER}/kafka_${SCALA_VER}-${KAFKA_VER}.tgz -O /tmp/kafka.tgz
sudo tar -xzf /tmp/kafka.tgz -C /usr/local
sudo mv /usr/local/kafka_${SCALA_VER}-${KAFKA_VER} ${DEST}
echo "Kafka installed to ${DEST}"
