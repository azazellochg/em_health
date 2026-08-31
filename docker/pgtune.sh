#!/bin/bash
set -euo pipefail


CONF="${PGDATA}/postgresql.conf"

log() {
    printf '%s: %s -> %s\n' "$1" "$2" "$3"
}

get_memory() {
    if [[ -f /sys/fs/cgroup/memory.max ]]; then
        # cgroup v2
        local limit
        limit=$(cat /sys/fs/cgroup/memory.max)

        if [[ "$limit" != "max" ]]; then
            echo "$limit"
            return
        fi
    fi

    # Fallback
    awk '/MemTotal/ { print $2 * 1024 }' /proc/meminfo
}

get_pg_version() {
    psql -Atqc "SELECT current_setting('server_version_num')::int / 10000"
}

get_cfg_value() {
    psql -Atqc "SHOW $1"
}


# Get input values
MEMORY_BYTES=$(get_memory)
CPUS=$(nproc)
PG_VERSION=$(get_pg_version)

# Compute recommendations

cmp_max_conns() {
    local GB=$((1024 * 1024 * 1024))
    local min_max_conns=20
    local max_connections_default=100

    if (( $MEMORY_BYTES <= 2 * GB )); then
        echo "$min_max_conns"
    elif (( $MEMORY_BYTES <= 4 * GB )); then
        echo 50
    elif (( $MEMORY_BYTES <= 6 * GB )); then
        echo 75
    else
        echo "$max_connections_default"
    fi
}

cmp_shared_buffers() {
  # 25% RAM, return Mb
  echo $(( MEMORY_BYTES / 4 / 1024 / 1024 ))
}

cmp_cache_size() {
  # 75% RAM, return Mb
  echo $(( (MEMORY_BYTES * 3) / 4 / 1024 / 1024 ))
}

cmp_work_mem() {
    local shared_buffers=$(( MEMORY_BYTES / 4 ))
    local cpus=$(nproc)
    # return kB
    echo $(( (MEMORY_BYTES - shared_buffers) / (16 * cpus * 1024) ))
}

cmp_max_parallel_workers() {
  local cpus=$(nproc)
  local value=$(( cpus / 2 ))

  if (( $value > 4 )); then
    echo 4
  else
    echo $value
  fi
}

cmp_vac_workers() {
  # 25% CPUs
  local cpus=$(nproc)
  echo $(( cpus / 4 ))
}

cmp_worker_processes() {
  #max_parallel_workers(cpus) + TS background workers + max_parallel_maintenance_workers
  local maint=$(cmp_max_parallel_workers)
  local cpus=$(nproc)
  echo $(( cpus + maint + 16 ))
}

cmp_max_locks_per_transaction() {
    local gb=$((1024 * 1024 * 1024))

    if (( MEMORY_BYTES >= 32 * gb )); then
        echo 1024
    elif (( MEMORY_BYTES >= 16 * gb )); then
        echo 512
    elif (( MEMORY_BYTES >= 8 * gb )); then
        echo 256
    else
        echo 128
    fi
}


printf "\nEnvironment:\n===========\n"
printf "Memory: $((MEMORY_BYTES / 1024 / 1024)) MiB\n"
printf "CPUs: $CPUS\n"
printf "PostgreSQL version: $PG_VERSION\n"
printf "Assuming you are using SSD drive\n"

printf "\nPostgreSQL settings:\n===================="
printf "\n---memory---\n"
log "shared_buffers" $(get_cfg_value "shared_buffers") $(cmp_shared_buffers)MB
log "effective_cache_size" $(get_cfg_value "effective_cache_size") $(cmp_cache_size)MB
log "work_mem" $(get_cfg_value "work_mem") $(cmp_work_mem)kB
log "maintenance_work_mem" $(get_cfg_value "maintenance_work_mem") "1024MB"
log "max_connections" $(get_cfg_value "max_connections") $(cmp_max_conns)
log "timescaledb.max_background_workers" $(get_cfg_value "timescaledb.max_background_workers") 16
log "max_parallel_workers" $(get_cfg_value "max_parallel_workers") $(nproc)
log "max_worker_processes" $(get_cfg_value "max_worker_processes") $(cmp_worker_processes)
log "max_parallel_maintenance_workers" $(get_cfg_value "max_parallel_maintenance_workers") $(cmp_max_parallel_workers)
log "max_parallel_workers_per_gather" $(get_cfg_value "max_parallel_workers_per_gather") $(( $(nproc) / 2 ))

printf "\n---autovacuum---\n"
log "autovacuum_work_mem" $(get_cfg_value "autovacuum_work_mem") "512MB" # for each worker
log "autovacuum_max_workers" $(get_cfg_value "autovacuum_max_workers") $(cmp_vac_workers)
log "autovacuum_worker_slots" $(get_cfg_value "autovacuum_worker_slots") $(cmp_vac_workers)
log "autovacuum_naptime" $(get_cfg_value "autovacuum_naptime") "30s"
log "autovacuum_vacuum_threshold" $(get_cfg_value "autovacuum_vacuum_threshold") 1000 # to skip very small tables
log "autovacuum_vacuum_scale_factor" $(get_cfg_value "autovacuum_vacuum_scale_factor") "0.01" # set all scales to 1-2%
log "autovacuum_vacuum_insert_scale_factor" $(get_cfg_value "autovacuum_vacuum_insert_scale_factor") "0.01"
log "autovacuum_analyze_scale_factor" $(get_cfg_value "autovacuum_analyze_scale_factor") "0.02"
log "autovacuum_vacuum_cost_delay" $(get_cfg_value "autovacuum_vacuum_cost_delay") "1ms"
log "autovacuum_vacuum_cost_limit" $(get_cfg_value "autovacuum_vacuum_cost_limit") 2000 # shared between workers

printf "\n---checkpoints and WAL---\n"
log "checkpoint_timeout" $(get_cfg_value "checkpoint_timeout") "30min"
log "checkpoint_completion_target" $(get_cfg_value "checkpoint_completion_target") "0.9"
log "wal_buffers" $(get_cfg_value "wal_buffers") "16Mb"
log "min_wal_size" $(get_cfg_value "min_wal_size") "4Gb"
log "max_wal_size" $(get_cfg_value "max_wal_size") "20Gb"
log "wal_compression" $(get_cfg_value "wal_compression") "lz4"

printf "\n---misc---\n"
log "default_statistics_target" $(get_cfg_value "default_statistics_target") 500
log "statement_timeout" $(get_cfg_value "statement_timeout") "15min"
log "max_locks_per_transaction" $(get_cfg_value "max_locks_per_transaction") $(cmp_max_locks_per_transaction)
log "effective_io_concurrency" $(get_cfg_value "effective_io_concurrency") 300
log "random_page_cost" $(get_cfg_value "random_page_cost") "1.1"
