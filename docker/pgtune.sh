#!/usr/bin/env bash
set -euo pipefail

YES=0
DRYRUN=0
RESTORE=0

Help()
{
    echo "Usage of pgtune.sh:"
    echo
    echo "-y|--yes        Show and apply changes"
    echo "-d|--dry-run    Show recommendations without applying changes"
    echo "-r|--restore    Restore the latest postgresql.auto.conf backup"
    echo "-h|--help       Show this message"
    echo
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -y|--yes)
            YES=1
            shift
            ;;
        -d|--dry-run)
            DRYRUN=1
            shift
            ;;
        -r|--restore)
            RESTORE=1
            shift
            ;;
        -h|--help)
            Help
            exit 0
            ;;
        --*|-*)
            echo "Unknown option: $1"
            exit 1
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

if [[ ! -d "${PGDATA:-}" ]]; then
    echo "PGDATA does not exist: ${PGDATA:-<unset>}"
    exit 1
fi

AUTO_CONF="${PGDATA}/postgresql.auto.conf"
PGTUNE_BACKUP_DIR="${PGDATA}/pgtune_backups"

reload_and_report() {
    local pending

    psql -v ON_ERROR_STOP=1 -Atqc "SELECT pg_reload_conf();" > /dev/null
    sleep 1

    pending=$(psql -v ON_ERROR_STOP=1 -Atqc \
        "SELECT string_agg(name, ', ' ORDER BY name) FROM pg_settings WHERE pending_restart;")

    if [[ -n "$pending" ]]; then
        printf '\nConfiguration reloaded. These settings need a restart to take effect:\n  %s\n' "$pending"
        printf 'Run e.g. "podman restart emhealth-db" (or docker).\n'
        printf 'During the first database initialization this happens automatically.\n'
    else
        printf '\nConfiguration reloaded, all settings are in effect. No restart needed.\n'
    fi
}

log() {
    printf '%s: %s -> %s\n' "$1" "$2" "$3"
}

get_memory() {
    if [[ -f /sys/fs/cgroup/memory.max ]]; then
        local limit
        limit=$(cat /sys/fs/cgroup/memory.max)

        if [[ "$limit" != "max" ]]; then
            echo "$limit"
            return
        fi
    fi

    awk '/MemTotal/ { print $2 * 1024 }' /proc/meminfo
}

get_pg_version() {
    psql -v ON_ERROR_STOP=1 -Atqc \
        "SELECT current_setting('server_version_num')::int / 10000"
}

get_pg_config() {
    local values

    values=$(psql -v ON_ERROR_STOP=1 -Atqc "
        SELECT
            current_setting('shared_buffers'),
            current_setting('effective_cache_size'),
            current_setting('work_mem'),
            current_setting('maintenance_work_mem'),
            current_setting('max_connections'),
            current_setting('timescaledb.max_background_workers'),
            current_setting('max_parallel_workers'),
            current_setting('max_worker_processes'),
            current_setting('max_parallel_maintenance_workers'),
            current_setting('max_parallel_workers_per_gather'),
            current_setting('temp_buffers'),
            current_setting('autovacuum_work_mem'),
            current_setting('autovacuum_max_workers'),
            current_setting('autovacuum_worker_slots'),
            current_setting('autovacuum_naptime'),
            current_setting('autovacuum_vacuum_threshold'),
            current_setting('autovacuum_vacuum_scale_factor'),
            current_setting('autovacuum_vacuum_insert_scale_factor'),
            current_setting('autovacuum_analyze_scale_factor'),
            current_setting('autovacuum_vacuum_cost_delay'),
            current_setting('autovacuum_vacuum_cost_limit'),
            current_setting('checkpoint_timeout'),
            current_setting('checkpoint_completion_target'),
            current_setting('wal_buffers'),
            current_setting('min_wal_size'),
            current_setting('max_wal_size'),
            current_setting('wal_compression'),
            current_setting('default_statistics_target'),
            current_setting('max_locks_per_transaction'),
            current_setting('effective_io_concurrency'),
            current_setting('random_page_cost'),
            current_setting('jit'),
            current_setting('idle_in_transaction_session_timeout'),
            current_setting('default_toast_compression')
    ")

    IFS='|' read -r \
        PG_SHARED_BUFFERS \
        PG_CACHE_SIZE \
        PG_WORK_MEM \
        PG_MAINT_WORK_MEM \
        PG_MAX_CONN \
        TS_MAX_BGWORKERS \
        PG_MAX_PAR_WRK \
        PG_MAX_WRK_PROC \
        PG_MAX_PAR_MAINT_WRK \
        PG_MAX_PAR_WRK_PER_GATHER \
        PG_TMP_BUFFERS \
        PG_AV_WORK_MEM \
        PG_AV_MAX_WRK \
        PG_AV_WRK_SLOTS \
        PG_AV_NAPTIME \
        PG_AV_THR \
        PG_AV_SCALE \
        PG_AV_INS_SCALE \
        PG_AV_ANALYZE_SCALE \
        PG_AV_COST_DELAY \
        PG_AV_COST_LIMIT \
        PG_CHK_TIMEOUT \
        PG_CHK_TGT \
        PG_WAL_BUFFERS \
        PG_MIN_WAL \
        PG_MAX_WAL \
        PG_WAL_CMP \
        PG_STAT_TGT \
        PG_MAX_LOCKS \
        PG_EFF_IO \
        PG_RND_PAGE_COST \
        PG_JIT \
        PG_IDLE_TX_TIMEOUT \
        PG_TOAST_CMP <<< "$values"
}

# --------------------------------------------------------------------
# Recommendation calculations
# --------------------------------------------------------------------

cmp_max_conns() {
    local GB=$((1024 * 1024 * 1024))
    local min_max_conns=20
    local max_connections_default=100

    if (( MEMORY_BYTES <= 2 * GB )); then
        echo "$min_max_conns"
    elif (( MEMORY_BYTES <= 4 * GB )); then
        echo 50
    elif (( MEMORY_BYTES <= 6 * GB )); then
        echo 75
    else
        echo "$max_connections_default"
    fi
}

cmp_shared_buffers() {
    # Start with 25% RAM, then optimize queries before changing it. Timescale uncompressed chunk indexes must fit into this
    # Return MB
    echo $(( MEMORY_BYTES / 4 / 1024 / 1024 ))
}

cmp_cache_size() {
    # 75% RAM, return MB
    echo $(( MEMORY_BYTES * 3 / 4 / 1024 / 1024 ))
}

cmp_maint_work_mem() {
    # 128MB per GB of RAM (as timescaledb-tune). Return MB
    local value=$(( MEMORY_BYTES / 1024 / 1024 / 8 ))

    if (( value > 2047 )); then
        echo 2047
    elif (( value < 64 )); then
        echo 64
    else
        echo "$value"
    fi
}

cmp_work_mem() {
    # 64-256Mb for analytical queries. If we run out, temp files will be created. We can increase this on the fly for heavy queries - check temp files in plans.
    # Memory for queries: RAM - shared_buffers - maintenance_work_mem - autovacuum workers' memory, with half of what's left kept for the OS page cache.
    # Divided among everything that can run a query at once (connections + parallel workers),
    # assuming 2 sort/hash steps per query, each up to hash_mem_multiplier (2) x work_mem.
    local ram_kb=$(( MEMORY_BYTES / 1024 ))
    local sb_kb=$(( ram_kb / 4 ))
    local maint_kb=$(( $(cmp_maint_work_mem) * 1024 ))   # maintenance_work_mem
    local av_kb=$(( $(cmp_vac_workers) * 512 * 1024 ))   # autovacuum_work_mem 512MB each
    local avail_kb=$(( (ram_kb - sb_kb - maint_kb - av_kb) / 2 ))
    local backends=$(( $(cmp_max_conns) + CPUS ))
    local wm=$(( avail_kb / (backends * 2 * 2) ))

    (( wm < 65536 )) && wm=65536
    (( wm > 262144 )) && wm=262144
    echo "$wm"
}

cmp_max_parallel_maint_workers() {
    local value=$(( CPUS / 2 ))

    if (( value > 4 )); then
        echo 4
    elif (( value < 1 )); then
        echo 1
    else
        echo "$value"
    fi
}

cmp_max_parallel_workers_per_gather() {
    local value=$(( CPUS / 4 ))

    if (( value > 4 )); then
        echo 4
    elif (( value < 1 )); then
        echo 1
    else
        echo "$value"
    fi
}

cmp_vac_workers() {
    # 25% CPUs, minimum 1.
    local value=$(( CPUS / 4 ))

    if (( value < 1 )); then
        echo 1
    else
        echo "$value"
    fi
}

cmp_ts_bg_workers() {
    # At least 16, but never lower a value that was raised on purpose
    if (( TS_MAX_BGWORKERS > 16 )); then
        echo "$TS_MAX_BGWORKERS"
    else
        echo 16
    fi
}

cmp_worker_processes() {
    # Base PostgreSQL workers + TimescaleDB background workers + PostgreSQL parallel workers.
    echo $(( 3 + $(cmp_ts_bg_workers) + CPUS ))
}

mem_at_least_gb() {
    # True if memory is at least $1 GB, allowing 5% slack: the kernel reserves part of the RAM,
    # so a "32 GB" host reports ~31 GB in /proc/meminfo
    local GB=$((1024 * 1024 * 1024))
    (( MEMORY_BYTES * 100 >= $1 * GB * 95 ))
}

cmp_max_locks_per_transaction() {
    if mem_at_least_gb 32; then
        echo 1024
    elif mem_at_least_gb 16; then
        echo 512
    elif mem_at_least_gb 8; then
        echo 256
    else
        echo 128
    fi
}

# --------------------------------------------------------------------
# Restore
# --------------------------------------------------------------------

if (( RESTORE )); then
    backup=$(ls -1t "${PGTUNE_BACKUP_DIR}"/postgresql.auto.conf.* 2>/dev/null | head -n 1 || true)

    if [[ -z "$backup" ]]; then
        echo "No postgresql.auto.conf backup found."
        exit 1
    fi

    cp -- "$backup" "$AUTO_CONF"
    echo "Restored $AUTO_CONF from $backup"
    reload_and_report
    exit 0
fi

# --------------------------------------------------------------------
# Get input values
# --------------------------------------------------------------------

MEMORY_BYTES=$(get_memory)
CPUS=$(nproc)
PG_VERSION=$(get_pg_version)

get_pg_config

printf "\nRecommendations based on %s MiB of available memory and %s CPUs for PostgreSQL %s\n" \
    "$((MEMORY_BYTES / 1024 / 1024))" \
    "$CPUS" \
    "$PG_VERSION"

printf "\n---Memory settings recommendations---\n"
log "shared_buffers" "$PG_SHARED_BUFFERS" "$(cmp_shared_buffers)MB"
log "effective_cache_size" "$PG_CACHE_SIZE" "$(cmp_cache_size)MB"
log "work_mem" "$PG_WORK_MEM" "$(cmp_work_mem)kB"
log "maintenance_work_mem" "$PG_MAINT_WORK_MEM" "$(cmp_maint_work_mem)MB"
log "max_connections" "$PG_MAX_CONN" "$(cmp_max_conns)"
log "timescaledb.max_background_workers" "$TS_MAX_BGWORKERS" "$(cmp_ts_bg_workers)"
log "max_parallel_workers" "$PG_MAX_PAR_WRK" "$CPUS"
log "max_worker_processes" "$PG_MAX_WRK_PROC" "$(cmp_worker_processes)"
log "max_parallel_maintenance_workers" "$PG_MAX_PAR_MAINT_WRK" "$(cmp_max_parallel_maint_workers)"
log "max_parallel_workers_per_gather" "$PG_MAX_PAR_WRK_PER_GATHER" "$(cmp_max_parallel_workers_per_gather)"
log "temp_buffers" "$PG_TMP_BUFFERS" "32MB"

printf "\n---Autovacuum settings recommendations---\n"
log "autovacuum_work_mem" "$PG_AV_WORK_MEM" "512MB" # for each worker
log "autovacuum_max_workers" "$PG_AV_MAX_WRK" "$(cmp_vac_workers)"
log "autovacuum_worker_slots" "$PG_AV_WRK_SLOTS" "$(cmp_vac_workers)"
log "autovacuum_naptime" "$PG_AV_NAPTIME" "30s" # <30s but consider logs crowding
log "autovacuum_vacuum_threshold" "$PG_AV_THR" 1000 # to prevent vacuuming small tables
log "autovacuum_vacuum_scale_factor" "$PG_AV_SCALE" "0.01"
log "autovacuum_vacuum_insert_scale_factor" "$PG_AV_INS_SCALE" "0.01"
log "autovacuum_analyze_scale_factor" "$PG_AV_ANALYZE_SCALE" "0.02"
log "autovacuum_vacuum_cost_delay" "$PG_AV_COST_DELAY" "1ms" # or 0ms on nvme
log "autovacuum_vacuum_cost_limit" "$PG_AV_COST_LIMIT" 2000 # shared between all workers

printf "\n---WAL settings recommendations---\n"
# Usually for best performance you would want more timed checkpoints rather than WAL-based checkpoints
log "checkpoint_timeout" "$PG_CHK_TIMEOUT" "30min"
log "checkpoint_completion_target" "$PG_CHK_TGT" "0.9"
log "wal_buffers" "$PG_WAL_BUFFERS" "16MB"
log "min_wal_size" "$PG_MIN_WAL" "512MB"
log "max_wal_size" "$PG_MAX_WAL" "4GB"
log "wal_compression" "$PG_WAL_CMP" "lz4"

printf "\n---Misc settings recommendations---\n"
log "default_statistics_target" "$PG_STAT_TGT" 500
log "max_locks_per_transaction" "$PG_MAX_LOCKS" "$(cmp_max_locks_per_transaction)"
log "effective_io_concurrency" "$PG_EFF_IO" 256
log "random_page_cost" "$PG_RND_PAGE_COST" "1.1" # 1.1 for SSD
log "jit" "$PG_JIT" "off"
log "default_toast_compression" "$PG_TOAST_CMP" "lz4"
log "idle_in_transaction_session_timeout" "$PG_IDLE_TX_TIMEOUT" "10min"

# --------------------------------------------------------------------
# Dry run
# --------------------------------------------------------------------

if (( DRYRUN )); then
    exit 0
fi

# --------------------------------------------------------------------
# Apply using ALTER SYSTEM
# --------------------------------------------------------------------

set_cfg_value() {
    local key="$1"
    local value="$2"

    psql -v ON_ERROR_STOP=1 -Atqc "ALTER SYSTEM SET ${key} TO ${value};"
}

apply() {
    local TS
    local BACKUP

    TS=$(date +%Y%m%d%H%M%S)
    BACKUP="${PGTUNE_BACKUP_DIR}/postgresql.auto.conf.${TS}"

    mkdir -p -m 700 -- "$PGTUNE_BACKUP_DIR"
    cp -- "$AUTO_CONF" "$BACKUP"
    printf '\nBackup: %s\nApplying configuration with ALTER SYSTEM...' "$BACKUP"

    # Memory
    set_cfg_value "shared_buffers" "'$(cmp_shared_buffers)MB'"
    set_cfg_value "effective_cache_size" "'$(cmp_cache_size)MB'"
    set_cfg_value "work_mem" "'$(cmp_work_mem)kB'"
    set_cfg_value "maintenance_work_mem" "'$(cmp_maint_work_mem)MB'"
    set_cfg_value "max_connections" "$(cmp_max_conns)"
    set_cfg_value "timescaledb.max_background_workers" "$(cmp_ts_bg_workers)"
    set_cfg_value "max_parallel_workers" "$CPUS"
    set_cfg_value "max_worker_processes" "$(cmp_worker_processes)"
    set_cfg_value "max_parallel_maintenance_workers" "$(cmp_max_parallel_maint_workers)"
    set_cfg_value "max_parallel_workers_per_gather" "$(cmp_max_parallel_workers_per_gather)"
    set_cfg_value "temp_buffers" "'32MB'"

    # Autovacuum
    set_cfg_value "autovacuum_work_mem" "'512MB'"
    set_cfg_value "autovacuum_max_workers" "$(cmp_vac_workers)"
    set_cfg_value "autovacuum_worker_slots" "$(cmp_vac_workers)"
    set_cfg_value "autovacuum_naptime" "'30s'"
    set_cfg_value "autovacuum_vacuum_threshold" "1000"
    set_cfg_value "autovacuum_vacuum_scale_factor" "0.01"
    set_cfg_value "autovacuum_vacuum_insert_scale_factor" "0.01"
    set_cfg_value "autovacuum_analyze_scale_factor" "0.02"
    set_cfg_value "autovacuum_vacuum_cost_delay" "'1ms'"
    set_cfg_value "autovacuum_vacuum_cost_limit" "2000"

    # WAL
    set_cfg_value "checkpoint_timeout" "'30min'"
    set_cfg_value "checkpoint_completion_target" "0.9"
    set_cfg_value "wal_buffers" "'16MB'"
    set_cfg_value "min_wal_size" "'512MB'"
    set_cfg_value "max_wal_size" "'4GB'"
    set_cfg_value "wal_compression" "'lz4'"

    # Misc
    set_cfg_value "default_statistics_target" "500"
    set_cfg_value "max_locks_per_transaction" "$(cmp_max_locks_per_transaction)"
    set_cfg_value "effective_io_concurrency" "256"
    set_cfg_value "random_page_cost" "1.1"
    set_cfg_value "jit" "off"
    set_cfg_value "default_toast_compression" "lz4"
    set_cfg_value "idle_in_transaction_session_timeout" "'10min'"

    printf ' done.\n'
    reload_and_report
}

# --------------------------------------------------------------------
# Apply confirmation
# --------------------------------------------------------------------

if (( YES )); then
    apply
    exit 0
fi

echo
echo "Do you want to apply the changes?"
select strictreply in "Yes" "No"; do
    relaxedreply=${strictreply:-${REPLY:-}}

    case "$relaxedreply" in
        Yes|yes|y)
            apply
            break
            ;;
        No|no|n)
            echo "No changes applied."
            exit 0
            ;;
        *)
            echo "Please answer yes or no."
            ;;
    esac
done
