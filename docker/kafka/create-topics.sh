#!/bin/bash
# Idempotent topic creation (--if-not-exists). Partition count is configurable (default 6).
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:19092}"
PARTITIONS="${KAFKA_PARTITIONS:-6}"
REPLICATION="${KAFKA_REPLICATION_FACTOR:-1}"
KT=/opt/kafka/bin/kafka-topics.sh

for i in $(seq 1 60); do
  "$KT" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1 && break
  echo "waiting for Kafka ($i/60)"; sleep 2
done

for topic in transaction.events transaction.risk.events transaction.invalid.events fraud.high-risk.alerts; do
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$PARTITIONS" --replication-factor "$REPLICATION"
done
"$KT" --bootstrap-server "$BOOTSTRAP" --describe | grep -E "^Topic: (transaction|fraud)\." || true
