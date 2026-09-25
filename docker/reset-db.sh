#!/bin/sh
set -eu

# This script drops selected database and recreates table structure from scratch

dbname="${1:?dbname required}"
do_init="${2:?do_init required (1/0)}"
version="${3:-}"

echo "Resetting database: $dbname"

psql -v ON_ERROR_STOP=1 -d postgres -v dbname="$dbname" <<'SQL'
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = :'dbname'
  AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS :"dbname";
CREATE DATABASE :"dbname";
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit CASCADE;
SQL

if [ -n "$version" ]; then
    psql -v ON_ERROR_STOP=1 -d "$dbname" -v version="$version" <<'SQL'
CREATE EXTENSION IF NOT EXISTS timescaledb VERSION :'version' CASCADE;
SQL
else
    psql -v ON_ERROR_STOP=1 -d "$dbname" <<'SQL'
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
SQL
fi

if [ "$do_init" -eq 1 ]; then
    echo "Creating initial db structure for: $dbname"
    psql -v ON_ERROR_STOP=1 \
        -d "$dbname" \
        -v var_data_chunk_size="'${TBL_DATA_CHUNK_SIZE}'" \
        -v var_pgstats_chunk_size="'${TBL_STATS_CHUNK_SIZE}'" \
        -v var_pgstats_retention="'${TBL_STATS_RETENTION}'" \
        -f /sql/init_db.sql
fi