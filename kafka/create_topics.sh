#!/bin/bash

# Create Kafka topics
docker exec -it kafka kafka-topics --create \
  --topic ecommerce_events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

docker exec -it kafka kafka-topics --create \
  --topic aggregated_results \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

echo "Kafka topics created successfully!"
