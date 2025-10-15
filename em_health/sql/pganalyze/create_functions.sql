-- get_db_stats
DROP FUNCTION IF EXISTS pganalyze.get_db_stats;
CREATE FUNCTION pganalyze.get_db_stats(job_id INT DEFAULT NULL, config JSONB DEFAULT NULL)
    RETURNS void
    LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO pganalyze.database_stats (
        collected_at,
        xact_commit, xact_rollback, blks_read, blks_hit,
        tup_inserted, tup_updated, tup_deleted, tup_fetched, tup_returned,
        temp_files, temp_bytes, deadlocks, blk_read_time, blk_write_time,
        frozen_xid_age, frozen_mxid_age, db_size
    )
    SELECT
        now() AS collected_at,
        s.xact_commit, s.xact_rollback, s.blks_read, s.blks_hit,
        s.tup_inserted, s.tup_updated, s.tup_deleted, s.tup_fetched, s.tup_returned,
        s.temp_files, s.temp_bytes, s.deadlocks, s.blk_read_time, s.blk_write_time,
        age(d.datfrozenxid) AS frozen_xid_age, mxid_age(d.datminmxid) AS frozen_mxid_age,
        pg_database_size(current_database()) AS db_size
    FROM pg_catalog.pg_stat_database s
             JOIN pg_catalog.pg_database d ON s.datname = d.datname
    WHERE s.datname = current_database();
END;
$$;

-- get_table_stats
DROP FUNCTION IF EXISTS pganalyze.get_table_stats;
CREATE FUNCTION pganalyze.get_table_stats(job_id INT DEFAULT NULL, config JSONB DEFAULT NULL)
    RETURNS void
    LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO pganalyze.table_stats (
        collected_at,
        relid,
        table_bytes,
        index_bytes,
        toast_bytes,
        frozen_xid_age,
        num_dead_rows,
        num_live_rows
    )
    SELECT
        now() AS collected_at,
        s.relid,
        CASE
            WHEN ht.hypertable_name IS NOT NULL
                THEN (hds).table_bytes
            ELSE pg_table_size(s.relid)
            END AS table_bytes,
        CASE
            WHEN ht.hypertable_name IS NOT NULL
                THEN (hds).index_bytes
            ELSE pg_indexes_size(s.relid)
            END AS index_bytes,
        CASE
            WHEN ht.hypertable_name IS NOT NULL
                THEN (hds).toast_bytes
            ELSE pg_total_relation_size(s.relid) - pg_table_size(s.relid) - pg_indexes_size(s.relid)
            END AS toast_external_bytes,
        age(c.relfrozenxid) AS frozen_xid_age,
        COALESCE(st.n_dead_tup, 0) AS num_dead_rows,
        COALESCE(st.n_live_tup, 0) AS num_live_rows
    FROM pg_catalog.pg_statio_user_tables s
             JOIN pg_catalog.pg_class c ON c.oid = s.relid
             LEFT JOIN pg_catalog.pg_stat_user_tables st ON st.relid = s.relid
             LEFT JOIN timescaledb_information.hypertables ht
                       ON ht.hypertable_schema = s.schemaname
                           AND ht.hypertable_name = s.relname
             LEFT JOIN LATERAL
        hypertable_detailed_size(ht.hypertable_schema || '.' || ht.hypertable_name) AS hds
                       ON ht.hypertable_name IS NOT NULL
    WHERE s.schemaname NOT LIKE '\_timescaledb%';
END;
$$;

-- get_index_stats
DROP FUNCTION IF EXISTS pganalyze.get_index_stats;
CREATE FUNCTION pganalyze.get_index_stats(job_id INT DEFAULT NULL, config JSONB DEFAULT NULL)
    RETURNS void
    LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO pganalyze.index_stats (
        collected_at,
        indexrelid,
        relid,
        size_bytes,
        scan,
        tup_read,
        tup_fetch,
        blks_read,
        blks_hit,
        exclusively_locked
    )
    WITH locked_relids AS (
        SELECT DISTINCT relation indexrelid
        FROM pg_catalog.pg_locks
        WHERE mode = 'AccessExclusiveLock' AND relation IS NOT NULL AND locktype = 'relation'
    )
    SELECT
        now() AS collected_at,
        s.indexrelid,
        s.relid,
        COALESCE(pg_catalog.pg_relation_size(s.indexrelid), 0) AS size_bytes,
        COALESCE(s.idx_scan, 0) AS scan,
        COALESCE(s.idx_tup_read, 0) AS tup_read,
        COALESCE(s.idx_tup_fetch, 0) AS tup_fetch,
        COALESCE(sio.idx_blks_read, 0) AS blks_read,
        COALESCE(sio.idx_blks_hit, 0) AS blks_hit,
        false AS exclusively_locked
    FROM pg_catalog.pg_stat_user_indexes s
             LEFT JOIN pg_catalog.pg_statio_user_indexes sio USING (indexrelid)
    WHERE NOT EXISTS (
        SELECT 1
        FROM locked_relids l
        WHERE l.indexrelid = s.indexrelid
    )
      AND s.schemaname NOT LIKE '\_timescaledb%'

    UNION ALL

    SELECT
        now() AS collected_at,
        l.indexrelid,
        c.relnamespace AS relid,
        0,
        0,
        0,
        0,
        0,
        0,
        true AS exclusively_locked
    FROM locked_relids l
             JOIN pg_class c ON c.oid = l.indexrelid;
END;
$$;

-- get_stat_statements
DROP FUNCTION IF EXISTS pganalyze.get_stat_statements;
CREATE FUNCTION pganalyze.get_stat_statements(job_id INT DEFAULT NULL, config JSONB DEFAULT NULL)
    RETURNS void
    LANGUAGE plpgsql
AS $$
DECLARE
    snapshot_time TIMESTAMPTZ := now();
BEGIN
    WITH statements AS (
        SELECT *
        FROM public.pg_stat_statements s
                 JOIN pg_database d ON d.oid = s.dbid
        WHERE userid IN ('grafana'::regrole::oid, 'emhealth'::regrole::oid)
          AND queryid IS NOT NULL
          AND d.datname = current_database()
    ),

         queries AS (
             INSERT INTO
                 pganalyze.queries (queryid, query)
                 SELECT
                     queryid, query
                 FROM
                     statements
                 ON CONFLICT
                     DO NOTHING
                 RETURNING
                     queryid
         ),

         snapshot AS (
             INSERT INTO
                 pganalyze.stat_snapshots
                 SELECT
                     snapshot_time AS collected_at,
                     sum(calls) AS calls,
                     sum(total_plan_time) AS total_plan_time,
                     sum(total_exec_time) AS total_exec_time,
                     sum(rows) AS rows,
                     sum(shared_blks_hit) AS shared_blks_hit,
                     sum(shared_blks_read) AS shared_blks_read,
                     sum(shared_blks_dirtied) AS shared_blks_dirtied,
                     sum(shared_blks_written) AS shared_blks_written,
                     sum(local_blks_hit) AS local_blks_hit,
                     sum(local_blks_read) AS local_blks_read,
                     sum(local_blks_dirtied) AS local_blks_dirtied,
                     sum(local_blks_written) AS local_blks_written,
                     sum(temp_blks_read) AS temp_blks_read,
                     sum(temp_blks_written) AS temp_blks_written,
                     sum(shared_blk_read_time) AS blk_read_time,
                     sum(shared_blk_write_time) AS blk_write_time,
                     sum(wal_records) AS wal_records,
                     sum(wal_fpi) AS wal_fpi,
                     sum(wal_bytes) AS wal_bytes,
                     pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0') AS wal_position,
                     pg_postmaster_start_time() AS stats_reset
                 FROM
                     statements
         )

    INSERT INTO
        pganalyze.stat_statements (collected_at,
                                   userid,
                                   queryid,
                                   plans,
                                   calls,
                                   total_plan_time,
                                   total_exec_time,
                                   mean_exec_time,
                                   rows,
                                   shared_blks_hit,
                                   shared_blks_read,
                                   shared_blks_dirtied,
                                   shared_blks_written,
                                   local_blks_hit,
                                   local_blks_read,
                                   local_blks_dirtied,
                                   local_blks_written,
                                   temp_blks_read,
                                   temp_blks_written,
                                   blk_read_time,
                                   blk_write_time,
                                   wal_records,
                                   wal_fpi,
                                   wal_bytes

    )
    SELECT
        snapshot_time,
        userid,
        queryid,
        plans,
        calls,
        total_plan_time,
        total_exec_time,
        mean_exec_time,
        rows,
        shared_blks_hit,
        shared_blks_read,
        shared_blks_dirtied,
        shared_blks_written,
        local_blks_hit,
        local_blks_read,
        local_blks_dirtied,
        local_blks_written,
        temp_blks_read,
        temp_blks_written,
        shared_blk_read_time,
        shared_blk_write_time,
        wal_records,
        wal_fpi,
        wal_bytes
    FROM
        statements;

END;
$$;

-- parse_logs
DROP FUNCTION IF EXISTS pganalyze.parse_logs;
CREATE FUNCTION pganalyze.parse_logs(job_id INT DEFAULT NULL, config JSONB DEFAULT NULL)
    RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
    logfile TEXT;
BEGIN
    -- Construct a full path to a log file
    logfile := current_setting('data_directory') || '/' ||
               (select pg_current_logfile('csvlog'));

    -- Create temp log table
    CREATE TEMP TABLE tmp_log (
                                  log_time timestamp(3) with time zone,
                                  user_name text,
                                  database_name text,
                                  process_id integer,
                                  connection_from text,
                                  session_id text,
                                  session_line_num bigint,
                                  command_tag text,
                                  session_start_time timestamp(3) with time zone,
                                  virtual_transaction_id text,
                                  transaction_id bigint,
                                  error_severity text,
                                  sql_state_code text,
                                  message text,
                                  detail text,
                                  hint text,
                                  internal_query text,
                                  internal_query_pos integer,
                                  context text,
                                  query text,
                                  query_pos integer,
                                  location text,
                                  application_name text,
                                  backend_type text,
                                  leader_pid integer,
                                  query_id bigint,
                                  PRIMARY KEY (session_id, session_line_num)
    ) ON COMMIT DROP;

    -- Load CSV log data
    EXECUTE format('COPY tmp_log FROM %L WITH CSV', logfile);

    -- Insert parsed vacuums into vacuum_stats
    INSERT INTO pganalyze.vacuum_stats (
        relid,
        started_at,
        finished_at,
        index_scans,
        pages_removed,
        tuples_removed,
        tuples_remain,
        wraparound,
        details
    )
    SELECT
        regexp_replace(
                substring(message FROM 'automatic vacuum of table "([^"]+)"'),
                '^[^.]+\.',  -- remove leading "dbname."
                ''
        )::regclass::oid AS relid,
        log_time AS started_at,
        log_time + (substring(message FROM 'elapsed: ([0-9\.]+) s')::double precision * interval '1 second') AS finished_at,
        substring(message FROM 'index scans: (\d+)')::bigint AS index_scans,
        substring(message FROM 'pages: (\d+) removed')::bigint AS pages_removed,
        substring(message FROM 'tuples: (\d+) removed')::bigint AS tuples_removed,
        substring(message FROM 'tuples: \d+ removed, (\d+) remain')::int AS tuples_remain,
        (message LIKE '%to prevent wraparound%') AS wraparound,
        message AS details
    FROM tmp_log
    WHERE error_severity = 'LOG'
      AND backend_type = 'autovacuum worker'
      AND (message LIKE 'automatic vacuum of table "' || current_database() || '.public.%'
        OR message LIKE 'automatic vacuum of table "' || current_database() || '.uec.%'
        OR message LIKE 'automatic vacuum of table "' || current_database() || '.pganalyze.%')
    ON CONFLICT DO NOTHING;

    -- Insert parsed plans into stat_explains
    INSERT INTO pganalyze.stat_explains (
        time,
        queryid,
        duration,
        total_cost,
        bytes_read,
        io_read_time,
        plan
    )
    SELECT
        log_time,
        query_id,
        substring(message FROM 'duration: ([\d.]+) ms')::double precision,
        (substring(message FROM 'plan:\n(\{.*)')::json #>> '{Plan,Total Cost}')::double precision AS total_cost,
        (substring(message FROM 'plan:\n(\{.*)')::json #>> '{Plan,Shared Read Blocks}')::bigint * 8192 AS bytes_read,
        (substring(message FROM 'plan:\n(\{.*)')::json #>> '{Plan,Shared I/O Read Time}')::double precision AS io_read_time,
        substring(message FROM 'plan:\n(\{.*)')::json
    FROM tmp_log
    WHERE database_name = current_database()
      AND user_name = 'grafana'
      AND error_severity = 'LOG'
      AND message LIKE 'duration: %'
    ON CONFLICT DO NOTHING;
END;
$$;

-- parse sysinfo
DROP FUNCTION IF EXISTS pganalyze.parse_sysinfo;
CREATE FUNCTION pganalyze.parse_sysinfo(job_id INT DEFAULT NULL, config JSONB DEFAULT NULL)
    RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
AS $$
BEGIN
    INSERT INTO pganalyze.sys_stats (load1, load5, load15, cpu_count, mem_total, mem_free, mem_avail)
    WITH loadavg AS (
        SELECT regexp_split_to_array(pg_read_file('/proc/loadavg', 0, 100), ' ') AS parts
    ),
         cpu AS (
             SELECT count(*) AS cpu_count
             FROM unnest(string_to_array(pg_read_file('/proc/stat', 0, 2000), E'\n')) AS line
             WHERE line ~ '^cpu[0-9]+'
         ),
         mem AS (
             SELECT
                 max(CASE WHEN line LIKE 'MemTotal:%' THEN trim(regexp_replace(split_part(line, ':', 2), '[^0-9]', '', 'g'))::bigint END) AS mem_total,
                 max(CASE WHEN line LIKE 'MemFree:%' THEN trim(regexp_replace(split_part(line, ':', 2), '[^0-9]', '', 'g'))::bigint END) AS mem_free,
                 max(CASE WHEN line LIKE 'MemAvailable:%' THEN trim(regexp_replace(split_part(line, ':', 2), '[^0-9]', '', 'g'))::bigint END) AS mem_avail
             FROM (
                      -- only read first 200 bytes of meminfo, which always covers first 3 lines
                      SELECT unnest(string_to_array(pg_read_file('/proc/meminfo', 0, 200), E'\n')) AS line
                  ) t
         )
    SELECT
        parts[1]::double precision AS load1,
        parts[2]::double precision AS load5,
        parts[3]::double precision AS load15,
        cpu_count,
        mem.mem_total,
        mem.mem_free,
        mem.mem_avail
    FROM loadavg, cpu, mem;
END;
$$;

-- Purge old data
DROP FUNCTION IF EXISTS pganalyze.purge_stats;
CREATE FUNCTION pganalyze.purge_stats(job_id INT DEFAULT NULL, config JSONB DEFAULT '{"drop_after":"6 months"}')
    RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
    drop_after interval;
BEGIN
    SELECT jsonb_object_field_text (config, 'drop_after')::interval
    INTO STRICT drop_after;

    IF drop_after IS NULL THEN
        RAISE EXCEPTION 'Config must have drop_after';
    END IF;

    DELETE FROM pganalyze.database_stats
    WHERE collected_at < NOW() - drop_after;

    DELETE FROM pganalyze.table_stats
    WHERE collected_at < NOW() - drop_after;

    DELETE FROM pganalyze.index_stats
    WHERE collected_at < NOW() - drop_after;

    DELETE FROM pganalyze.vacuum_stats
    WHERE started_at < NOW() - drop_after;

    DELETE FROM pganalyze.stat_explains
    WHERE time < NOW() - drop_after;

    DELETE FROM pganalyze.sys_stats
    WHERE time < NOW() - drop_after;
END;
$$;

GRANT USAGE ON SCHEMA pganalyze TO pganalyze;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA pganalyze TO pganalyze;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pganalyze TO pganalyze;
GRANT USAGE ON SCHEMA pganalyze TO grafana;
GRANT SELECT ON ALL TABLES IN SCHEMA pganalyze TO grafana;
ALTER DEFAULT PRIVILEGES IN SCHEMA pganalyze GRANT SELECT ON TABLES TO grafana;