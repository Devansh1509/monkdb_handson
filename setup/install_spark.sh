#!/bin/bash
# Minimal helper to install Spark for pyspark use in WSL (downloads binary tarball)
set -e
SPARK_VER="3.4.1"
HADOOP_VER="3"
DEST="/opt/spark"
echo "Downloading Spark..."
wget https://archive.apache.org/dist/spark/spark-${SPARK_VER}/spark-${SPARK_VER}-bin-hadoop${HADOOP_VER}.tgz -O /tmp/spark.tgz
sudo tar -xzf /tmp/spark.tgz -C /opt
sudo mv /opt/spark-${SPARK_VER}-bin-hadoop${HADOOP_VER} ${DEST}
echo "Spark installed to ${DEST}"
echo "You might need to set SPARK_HOME and add to PATH."
