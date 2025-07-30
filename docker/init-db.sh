#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE tem;
    CREATE DATABASE sem;
    CREATE ROLE grafana WITH LOGIN PASSWORD '${POSTGRES_GRAFANA_PASSWORD}';
    GRANT pg_stat_scan_tables TO grafana;
    GRANT pg_read_all_stats TO grafana;
EOSQL

for db in tem sem; do
  echo "Creating initial db structure for: $db"
  psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname="$db" \
  -v TBL_DATA_PARTITIONS=$TBL_DATA_PARTITIONS \
  -v TBL_DATA_CHUNK_INTERVAL="'$TBL_DATA_CHUNK_INTERVAL'" \
  -v TBL_DATA_COMPRESSION="'$TBL_DATA_COMPRESSION'" \
  -f /docker-entrypoint-initdb.d/init-tables.sql
done