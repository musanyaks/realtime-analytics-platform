#!/bin/bash
# Create Kafka topics for the real-time analytics platform

set -e

KAFKA_BROKER=${KAFKA_BROKER:-localhost:29092}

echo "Creating Kafka topics on $KAFKA_BROKER..."

# User events topic - high throughput, short retention for real-time processing
docker exec -it realtime-analytics-platform-kafka-1     kafka-topics --create     --bootstrap-server kafka:9092     --topic user_events     --partitions 6     --replication-factor 1     --config retention.ms=86400000     --config cleanup.policy=delete     --if-not-exists

# Transactions topic - critical data, longer retention
docker exec -it realtime-analytics-platform-kafka-1     kafka-topics --create     --bootstrap-server kafka:9092     --topic transactions     --partitions 3     --replication-factor 1     --config retention.ms=604800000     --config min.insync.replicas=1     --if-not-exists

# Clickstream topic - very high throughput
docker exec -it realtime-analytics-platform-kafka-1     kafka-topics --create     --bootstrap-server kafka:9092     --topic clickstream     --partitions 12     --replication-factor 1     --config retention.ms=43200000     --if-not-exists

echo "Topics created successfully!"
echo ""
echo "Listing all topics:"
docker exec -it realtime-analytics-platform-kafka-1     kafka-topics --list --bootstrap-server kafka:9092
