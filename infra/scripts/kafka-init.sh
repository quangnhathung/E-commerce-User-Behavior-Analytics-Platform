#!/usr/bin/env bash
set -euo pipefail
topics=/opt/kafka/bin/kafka-topics.sh
configs=/opt/kafka/bin/kafka-configs.sh
for spec in ecommerce.events.raw:3 ecommerce.events.dlq:1; do
  name=${spec%:*}
  partitions=${spec##*:}
  "$topics" --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 1 \
    --config retention.ms=172800000
  description=$("$topics" --bootstrap-server kafka:9092 --describe --topic "$name")
  if ! grep -Eq "PartitionCount:[[:space:]]*$partitions([[:space:]]|$)" <<< "$description" \
      || ! grep -Eq 'ReplicationFactor:[[:space:]]*1([[:space:]]|$)' <<< "$description"; then
    echo "Topic $name has unexpected partitions/replication; refusing to delete or recreate it." >&2
    exit 1
  fi
  config=$("$configs" --bootstrap-server kafka:9092 --entity-type topics --entity-name "$name" --describe)
  if ! grep -Eq 'retention.ms=172800000([,[:space:]]|$)' <<< "$config"; then
    echo "Topic $name has unexpected retention.ms; update it explicitly before restarting init." >&2
    exit 1
  fi
  echo "Verified $name ($partitions partitions, RF=1, retention=48h)"
done
