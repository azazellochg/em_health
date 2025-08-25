#!/bin/sh
set -e

psql -v ON_ERROR_STOP=1 <<EOSQL
    CREATE DATABASE tem;
    CREATE DATABASE sem;
    CREATE ROLE grafana WITH LOGIN PASSWORD '${POSTGRES_GRAFANA_PASSWORD}';
    GRANT pg_stat_scan_tables TO grafana;
    GRANT pg_read_all_stats TO grafana;
EOSQL

for db in tem sem; do
  echo "Creating initial db structure for: $db"
  psql -v ON_ERROR_STOP=1 --dbname="$db" -f /docker-entrypoint-initdb.d/init-tables.sql
done

echo "Running timescaledb-tune..."
timescaledb-tune --quiet --yes