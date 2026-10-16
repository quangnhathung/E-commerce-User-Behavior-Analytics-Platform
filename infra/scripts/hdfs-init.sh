#!/usr/bin/env bash
set -euo pipefail
ready=false
for attempt in $(seq 1 60); do
  status=$(curl -fsS --max-time 4 'http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState' || true)
  if grep -Eq '"NumLiveDataNodes"[[:space:]]*:[[:space:]]*2([,[:space:]]|$)' <<< "$status" \
      && hdfs dfsadmin -safemode get 2>/dev/null | grep -q 'Safe mode is OFF'; then
    ready=true
    break
  fi
  sleep 2
done
if [[ "$ready" != true ]]; then
  echo 'HDFS did not reach two live DataNodes with safe mode OFF.' >&2
  exit 1
fi
hdfs dfs -mkdir -p /ecommerce/{bronze,silver,gold,models,checkpoints,spark-events,_smoke}
hdfs dfs -chmod 750 /ecommerce
echo 'HDFS directories ready; existing data preserved.'
