#!/bin/bash

KAFKA_BROKER="kafka:29092"
TOPICS=(
  "nyc_taxi_stream"
  "nyc_taxi_dlq"
  "nyc_taxi_failed"
)

echo "Waiting for Kafka to be ready..."
while ! kafka-topics --bootstrap-server $KAFKA_BROKER --list; do
  echo "Kafka not ready yet. Retrying in 5 seconds..."
  sleep 5
done

echo "Creating topics..."
for TOPIC in "${TOPICS[@]}"; do
  kafka-topics --create \
    --bootstrap-server $KAFKA_BROKER \
    --topic $TOPIC \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --if-not-exists
done

echo "Topics created:"
kafka-topics --list --bootstrap-server $KAFKA_BROKER

echo "Topic configurations:"
for TOPIC in "${TOPICS[@]}"; do
  kafka-topics --describe --bootstrap-server $KAFKA_BROKER --topic $TOPIC
done