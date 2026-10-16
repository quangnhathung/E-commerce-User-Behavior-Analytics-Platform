#!/usr/bin/env bash
set -euo pipefail

role="${1:?Expected namenode or datanode}"
case "$role" in
  namenode) storage=/var/lib/hadoop/name ;;
  datanode) storage=/var/lib/hadoop/data ;;
  *) echo "Unknown Hadoop role: $role" >&2; exit 2 ;;
esac
: "${HDFS_CLUSTER_ID:?HDFS_CLUSTER_ID is required}"
version="$storage/current/VERSION"
if [[ -f "$version" ]]; then
  actual=$(sed -n 's/^clusterID=//p' "$version")
  if [[ "$actual" != "$HDFS_CLUSTER_ID" ]]; then
    echo "HDFS cluster ID mismatch in $storage; restore the matching .env/volume. Refusing to reformat." >&2
    exit 1
  fi
elif [[ "$role" == namenode ]]; then
  if [[ -n "$(find "$storage" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "NameNode storage is nonempty without current/VERSION; refusing to format." >&2
    exit 1
  fi
  hdfs namenode -format -nonInteractive -clusterid "$HDFS_CLUSTER_ID"
fi
exec hdfs "$role"
