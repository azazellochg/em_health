#!/bin/sh
set -eu

echo "Create or upgrade pgBackRest stanza..."
pgbackrest --stanza=main stanza-create || :
pgbackrest --stanza=main stanza-upgrade
pgbackrest --stanza=main check

psql -v ON_ERROR_STOP=1 <<EOSQL
    CREATE DATABASE tem;
    CREATE DATABASE sem;
    CREATE ROLE grafana WITH LOGIN PASSWORD '${POSTGRES_GRAFANA_PASSWORD}';
    CREATE ROLE emhealth WITH LOGIN PASSWORD '${POSTGRES_EMHEALTH_PASSWORD}';
    CREATE ROLE pganalyze WITH LOGIN PASSWORD '${POSTGRES_PGANALYZE_PASSWORD}' CONNECTION LIMIT 5;
    GRANT pg_stat_scan_tables TO grafana;
    GRANT pg_read_all_stats TO grafana;
    GRANT pg_monitor, pg_read_server_files TO pganalyze;
EOSQL

for db in tem sem; do
  echo "Creating initial db structure for: $db"
  psql -v ON_ERROR_STOP=1 \
  -v var_data_chunk_size="'${TBL_DATA_CHUNK_SIZE}'" \
  -v var_pgstats_chunk_size="'${TBL_STATS_CHUNK_SIZE}'" \
  -v var_pgstats_retention="'${TBL_STATS_RETENTION}'" \
  --dbname="$db" -f /sql/init_db.sql
done

echo "Running timescaledb-tune..."
timescaledb-tune -quiet -yes
