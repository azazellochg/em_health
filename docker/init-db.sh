#!/bin/sh
set -eu

# Helper to read a Docker secret from /run/secrets/<name>
read_secret() {
    name="$1"
    path="/run/secrets/$name"
    if [ -f "$path" ]; then
        cat "$path"
    else
        echo "Error: Secret file $path not found" >&2
        exit 1
    fi
}

# Read secrets
POSTGRES_GRAFANA_PASSWORD=$(read_secret postgres_grafana_password)
POSTGRES_EMHEALTH_PASSWORD=$(read_secret postgres_emhealth_password)
POSTGRES_PGANALYZE_PASSWORD=$(read_secret postgres_pganalyze_password)

# Create pgBackRest stanza
pgbackrest --stanza=main stanza-create
pgbackrest --stanza=main check

psql -v ON_ERROR_STOP=1 <<EOSQL
    CREATE DATABASE tem;
    CREATE DATABASE sem;
    CREATE ROLE grafana WITH LOGIN PASSWORD '${POSTGRES_GRAFANA_PASSWORD}';
    CREATE ROLE emhealth WITH LOGIN PASSWORD '${POSTGRES_EMHEALTH_PASSWORD}';
    CREATE ROLE pganalyze WITH LOGIN PASSWORD '${POSTGRES_PGANALYZE_PASSWORD}' CONNECTION LIMIT 5;
    GRANT pg_stat_scan_tables TO grafana;
    GRANT pg_read_all_stats TO grafana;
    GRANT pg_monitor TO pganalyze;
EOSQL

for db in tem sem; do
  echo "Creating initial db structure for: $db"
  psql -v ON_ERROR_STOP=1 \
  -v var_data_chunk_size="'${TBL_DATA_CHUNK_SIZE}'" \
  -v var_data_compression="'${TBL_DATA_COMPRESSION}'" \
  -v var_pgsnaps_chunk_size="'${TBL_SNAPS_CHUNK_SIZE}'" \
  -v var_pgstats_chunk_size="'${TBL_STATS_CHUNK_SIZE}'" \
  -v var_pgstats_compression="'${TBL_STATS_COMPRESSION}'" \
  -v var_pgstats_retention="'${TBL_STATS_RETENTION}'" \
  --dbname="$db" -f /sql/init_db.sql
done

for db in tem sem; do
  echo "Scheduling jobs as pganalyze user for db: $db"
  PGPASSWORD="${POSTGRES_PGANALYZE_PASSWORD}" \
  psql -v ON_ERROR_STOP=1 \
  -U pganalyze \
  --dbname="$db" -f /sql/pganalyze/create_jobs.sql
done

echo "Running timescaledb-tune..."
timescaledb-tune --quiet --yes
