#!/bin/sh
set -eu

# This script drops selected database and recreates table structure from scratch

dbname="${1:?dbname required}"
do_init="${2:?do_init required (1/0)}"
version_clause="${3:-}"

echo "Closing active connections for: ${dbname}"
psql -d postgres -c "SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '${dbname}';"

psql -d postgres -c "DROP DATABASE IF EXISTS \"${dbname}\";"
psql -d postgres -c "CREATE DATABASE \"${dbname}\";"

if [ -n "${version_clause}" ]; then
  psql -d "${dbname}" -c "CREATE EXTENSION IF NOT EXISTS timescaledb${version_clause} CASCADE;"
else
  psql -d "${dbname}" -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"
fi

psql -d "${dbname}" -c "CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit CASCADE;"

if [ "${do_init}" -eq 1 ]; then
  echo "Creating initial db structure for: ${dbname}"

  psql -v ON_ERROR_STOP=1 --dbname="${dbname}" \
    -v var_data_chunk_size="'${TBL_DATA_CHUNK_SIZE}'" \
    -v var_pgstats_chunk_size="'${TBL_STATS_CHUNK_SIZE}'" \
    -v var_pgstats_retention="'${TBL_STATS_RETENTION}'" \
    -f /sql/init_db.sql
fi