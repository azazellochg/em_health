#!/bin/sh
set -eu

echo "Create or upgrade pgBackRest stanza..."
pgbackrest --stanza=main stanza-upgrade || pgbackrest --stanza=main stanza-create
pgbackrest --stanza=main check

echo "Create databases and roles..."
# Passwords are read from the environment with \getenv and quoted by psql (:'var'),
# so any character is safe.
psql -v ON_ERROR_STOP=1 <<'EOSQL'
    \getenv grafana_pw POSTGRES_GRAFANA_PASSWORD
    \getenv emhealth_pw POSTGRES_EMHEALTH_PASSWORD
    \getenv pganalyze_pw POSTGRES_PGANALYZE_PASSWORD
    CREATE DATABASE tem;
    CREATE DATABASE sem;
    CREATE ROLE grafana WITH LOGIN PASSWORD :'grafana_pw' CONNECTION LIMIT 100;
    CREATE ROLE emhealth WITH LOGIN PASSWORD :'emhealth_pw' CONNECTION LIMIT 100;
    CREATE ROLE pganalyze WITH LOGIN PASSWORD :'pganalyze_pw' CONNECTION LIMIT 5;
    GRANT pg_monitor, pg_read_server_files TO pganalyze;
EOSQL

# Latest schema version = highest /sql/migrations/NNN.sql (same rule as em_health/utils/maintenance.py)
schema_version=$(ls /sql/migrations | awk -F. '/^[0-9][0-9][0-9]\.sql$/ { v = $1 + 0; if (v > max) max = v } END { if (max) print max }')
if [ -z "$schema_version" ]; then
  echo "Error: no migrations found in /sql/migrations" >&2
  exit 1
fi

for db in tem sem; do
  echo "Creating initial db structure for: $db"
  psql -v ON_ERROR_STOP=1 \
  -v var_data_chunk_size="${TBL_DATA_CHUNK_SIZE}" \
  -v var_pgstats_chunk_size="${TBL_STATS_CHUNK_SIZE}" \
  -v var_pgstats_retention="${TBL_STATS_RETENTION}" \
  -v var_schema_version="$schema_version" \
  --dbname="$db" -f /sql/init_db.sql
done

echo "Running pgtune.sh..."
/usr/local/bin/pgtune.sh -y