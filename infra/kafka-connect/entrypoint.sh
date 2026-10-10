#!/usr/bin/env bash
set -e

# Run background auto-initializer for Kafka topics and MongoDB connector
(
  # CLI tools spawn their own JVM inside this container; keep them small
  export KAFKA_HEAP_OPTS="-Xmx128M"
  echo "=== [Auto-Init] Waiting for Kafka Broker on kafka:9092 ==="
  until /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list > /dev/null 2>&1; do
    sleep 2
  done

  echo "=== [Auto-Init] Ensuring Kafka Topics ==="
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic gdelt.events.v1 --partitions 3 --replication-factor 1 \
    --config retention.ms=604800000 2>/dev/null || true

  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic gdelt.dlq.v1 --partitions 1 --replication-factor 1 \
    --config retention.ms=1209600000 2>/dev/null || true

  echo "=== [Auto-Init] Waiting for Kafka Connect REST API on localhost:8083 ==="
  until wget -q -O - http://127.0.0.1:8083/connectors > /dev/null 2>&1; do
    sleep 3
  done

  if ! wget -q -O - http://127.0.0.1:8083/connectors | grep -q "mongo-sink-market-data"; then
    echo "=== [Auto-Init] Registering MongoDB Sink Connector ==="
    wget -q --header="Content-Type: application/json" \
         --post-file=/opt/kafka/config/mongo-sink.json \
         -O - http://127.0.0.1:8083/connectors || true
    echo ""
  else
    echo "=== [Auto-Init] Connector mongo-sink-market-data is already active ==="
  fi
) &

echo "=== Starting Kafka Connect Distributed Worker ==="
exec /opt/kafka/bin/connect-distributed.sh /opt/kafka/config/connect-distributed.properties
