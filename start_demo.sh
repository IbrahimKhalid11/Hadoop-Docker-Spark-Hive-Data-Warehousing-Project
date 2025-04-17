#!/bin/bash

DOCKER_COMPOSE_FILE=docker-compose.yml
SCRIPT_PATH=$(cd "$(dirname "$0")"; pwd)

echo "Stopping and removing existing containers..."
docker-compose -f "${SCRIPT_PATH}/${DOCKER_COMPOSE_FILE}" down
docker-compose -f "${SCRIPT_PATH}/${DOCKER_COMPOSE_FILE}" pull

echo "Starting containers..."
docker-compose -f "${SCRIPT_PATH}/${DOCKER_COMPOSE_FILE}" up -d

echo "Waiting for containers to initialize..."
sleep 30

echo "Checking if HDFS is ready..."
until docker exec namenode hdfs dfsadmin -report; do
  echo "HDFS not ready yet. Retrying in 5 seconds..."
  sleep 5
done

echo "Creating /data and /user directories on HDFS..."
docker exec namenode bash -c "hdfs dfs -mkdir -p /data && hdfs dfs -mkdir -p /user/hive/warehouse"
docker exec namenode bash -c "hdfs dfs -chmod -R 777 /data && hdfs dfs -chmod -R 777 /user/hive/warehouse"

echo "Submitting Spark jobs..."

echo "Submitting Bronze.py..."
docker exec spark bash -c "spark-submit /Scripts/Bronze.py"

echo "Submitting Silver.py..."
docker exec spark bash -c "spark-submit /Scripts/Silver.py"

echo "Submitting Gold.py..."
docker exec spark bash -c "spark-submit /Scripts/Gold.py"

echo "Script completed successfully."