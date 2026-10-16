#!/usr/bin/env bash
set -euo pipefail
: "${CLUSTER_ID:?CLUSTER_ID is required}"
metadata=/var/lib/kafka/data/meta.properties
if [[ -f "$metadata" ]]; then
  actual=$(sed -n 's/^cluster.id=//p' "$metadata")
  if [[ "$actual" != "$CLUSTER_ID" ]]; then
    echo 'Kafka cluster ID differs from persisted storage; restore matching .env. Refusing to start.' >&2
    exit 1
  fi
fi
exec /etc/kafka/docker/run
