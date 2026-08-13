-- get_db_stats
DROP FUNCTION IF EXISTS pganalyze.get_db_stats;

CREATE FUNCTION pganalyze.get_db_stats(job_id INT = NULL, config jsonb = NULL) RETURNS VOID
  LANGUAGE plpgsql AS
$$
BEGIN
  INSERT INTO pganalyze.database_stats (
    collected_at,
    xact_commit,
    xact_rollback,
    blks_read,
    blks_hit,
    tup_inserted,
    tup_updated,
    tup_deleted,
    tup_fetched,
    tup_returned,
    temp_files,
    temp_bytes,
    deadlocks,
    blk_read_time,
    blk_write_time,
    frozen_xid_age,
    frozen_mxid_age,
    db_size,
    wal_lsn
  )
  SELECT
    NOW() AS collected_at,
    s.xact_commit,
    s.xact_rollback,
    s.blks_read,
    s.blks_hit,
    s.tup_inserted,
    s.tup_updated,
    s.tup_deleted,
    s.tup_fetched,
    s.tup_returned,
    s.temp_files,
    s.temp_bytes,
    s.deadlocks,
    s.blk_read_time,
    s.blk_write_time,
    AGE(d.datfrozenxid) AS frozen_xid_age,
    mxid_age(d.datminmxid) AS frozen_mxid_age,
    PG_DATABASE_SIZE(CURRENT_DATABASE()) AS db_size,
    pg_current_wal_lsn() AS wal_lsn
  FROM
    pg_catalog.pg_stat_database s
    JOIN pg_catalog.pg_database d
      ON s.datname = d.datname
  WHERE
    s.datname = CURRENT_DATABASE();
END;
$$;

/* get_table_stats
For tables, we omit materialized views; for hypertables, we omit CAGGs and group only uncompressed chunks
 */
DROP FUNCTION IF EXISTS pganalyze.get_table_stats;

CREATE FUNCTION pganalyze.get_table_stats(job_id INT = NULL, config jsonb = NULL) RETURNS VOID
  LANGUAGE plpgsql AS
$$
BEGIN
  WITH chunk_stats AS (
    SELECT
      ch.hypertable_schema,
      ch.hypertable_name,
      MAX(AGE(c.relfrozenxid)) AS frozen_xid_age,
      COALESCE(SUM(s.n_dead_tup), 0) AS num_dead_rows,
      COALESCE(SUM(s.n_live_tup), 0) AS num_live_rows
    FROM
      timescaledb_information.chunks ch
      JOIN pg_namespace n
        ON n.nspname = ch.chunk_schema
      JOIN pg_class c
        ON c.relnamespace = n.oid AND c.relname = ch.chunk_name
      LEFT JOIN pg_stat_user_tables s
        ON s.relid = c.oid
    WHERE
      NOT ch.is_compressed
      AND ch.hypertable_schema NOT LIKE '\_timescaledb%'
    GROUP BY
      ch.hypertable_schema,
      ch.hypertable_name
  )
  INSERT
  INTO pganalyze.table_stats (
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
    NOW() AS collected_at,
    s.relid,
    CASE WHEN ht.hypertable_name IS NOT NULL THEN (hds).table_bytes ELSE PG_TABLE_SIZE(s.relid) END AS table_bytes,
    CASE WHEN ht.hypertable_name IS NOT NULL THEN (hds).index_bytes ELSE PG_INDEXES_SIZE(s.relid) END AS index_bytes,
    CASE WHEN ht.hypertable_name IS NOT NULL THEN (hds).toast_bytes
         ELSE PG_TOTAL_RELATION_SIZE(s.relid) - PG_TABLE_SIZE(s.relid) -
              PG_INDEXES_SIZE(s.relid) END AS toast_external_bytes,
    CASE WHEN ht.hypertable_name IS NOT NULL THEN COALESCE(cs.frozen_xid_age, 0) ELSE COALESCE(AGE(c.relfrozenxid), 0) END AS frozen_xid_age,
    CASE WHEN ht.hypertable_name IS NOT NULL THEN COALESCE(cs.num_dead_rows, 0) ELSE COALESCE(st.n_dead_tup, 0) END AS num_dead_rows,
    CASE WHEN ht.hypertable_name IS NOT NULL THEN COALESCE(cs.num_live_rows, 0) ELSE COALESCE(st.n_live_tup, 0) END AS num_live_rows

  FROM
    pg_catalog.pg_statio_user_tables s
    JOIN pg_catalog.pg_class c
      ON c.oid = s.relid
    LEFT JOIN pg_catalog.pg_stat_user_tables st
      ON st.relid = s.relid
    LEFT JOIN timescaledb_information.hypertables ht
      ON ht.hypertable_schema = s.schemaname AND ht.hypertable_name = s.relname
    LEFT JOIN LATERAL hypertable_detailed_size(ht.hypertable_schema || '.' || ht.hypertable_name) AS hds
      ON ht.hypertable_name IS NOT NULL
    LEFT JOIN chunk_stats cs
      ON cs.hypertable_schema = ht.hypertable_schema AND cs.hypertable_name = ht.hypertable_name
  WHERE
    s.schemaname NOT LIKE '\_timescaledb%'
    AND c.relkind = 'r';
END;
$$;

/* get_index_stats
For chunks, we map the physical chunk index to the logical hypertable index. CAGGs are skipped.
 */
DROP FUNCTION IF EXISTS pganalyze.get_index_stats;

CREATE FUNCTION pganalyze.get_index_stats(job_id INT = NULL, config jsonb = NULL) RETURNS VOID
  LANGUAGE plpgsql AS
$$
BEGIN
  WITH regular_indexes AS (
    SELECT
      s.indexrelid,
      s.indexrelname,
      s.relid,
      s.schemaname,
      s.relname,
      PG_RELATION_SIZE(s.indexrelid) AS size_bytes,
      COALESCE(s.idx_scan, 0) AS idx_scan,
      COALESCE(s.idx_tup_read, 0) AS idx_tup_read,
      COALESCE(s.idx_tup_fetch, 0) AS idx_tup_fetch,
      COALESCE(io.idx_blks_read, 0) AS idx_blks_read,
      COALESCE(io.idx_blks_hit, 0) AS idx_blks_hit
    FROM
      pg_stat_user_indexes s
      LEFT JOIN pg_statio_user_indexes io
        USING (indexrelid)
    WHERE
      s.schemaname IN ('events', 'uec', 'pganalyze')
  ),

    chunk_indexes AS (
      SELECT
        REGEXP_REPLACE(s.indexrelname, '^(_hyper_[0-9]+_[0-9]+_chunk|[0-9]+_[0-9]+)_', '') AS indexrelname,
        s.relid,
        ch.hypertable_schema,
        ch.hypertable_name,
        PG_RELATION_SIZE(s.indexrelid) AS size_bytes,
        COALESCE(s.idx_scan, 0) AS idx_scan,
        COALESCE(s.idx_tup_read, 0) AS idx_tup_read,
        COALESCE(s.idx_tup_fetch, 0) AS idx_tup_fetch,
        COALESCE(io.idx_blks_read, 0) AS idx_blks_read,
        COALESCE(io.idx_blks_hit, 0) AS idx_blks_hit
      FROM
        pg_stat_user_indexes s
        JOIN timescaledb_information.chunks ch
          ON ch.chunk_schema = s.schemaname AND ch.chunk_name = s.relname
        LEFT JOIN pg_statio_user_indexes io
          USING (indexrelid)
      WHERE
        ch.hypertable_schema NOT LIKE '\_timescaledb%'
    ),

    chunk_sums AS (
      SELECT
        ch.hypertable_schema AS schemaname,
        ch.hypertable_name AS relname,
        ch.indexrelname,
        SUM(ch.size_bytes) AS size_bytes,
        SUM(ch.idx_scan) AS idx_scan,
        SUM(ch.idx_tup_read) AS idx_tup_read,
        SUM(ch.idx_tup_fetch) AS idx_tup_fetch,
        SUM(ch.idx_blks_read) AS idx_blks_read,
        SUM(ch.idx_blks_hit) AS idx_blks_hit
      FROM
        chunk_indexes ch
      GROUP BY
        ch.hypertable_schema,
        ch.hypertable_name,
        ch.indexrelname
    )

  INSERT
  INTO pganalyze.index_stats (
    collected_at,
    indexrelid,
    relid,
    size_bytes,
    scan,
    tup_read,
    tup_fetch,
    blks_read,
    blks_hit
  )
  SELECT
    NOW() AS collected_at,
    r.indexrelid,
    r.relid,
    r.size_bytes + COALESCE(ch.size_bytes, 0) AS size_bytes,
    r.idx_scan + COALESCE(ch.idx_scan, 0) AS idx_scan,
    r.idx_tup_read + COALESCE(ch.idx_tup_read, 0) AS idx_tup_read,
    r.idx_tup_fetch + COALESCE(ch.idx_tup_fetch, 0) AS idx_tup_fetch,
    r.idx_blks_read + COALESCE(ch.idx_blks_read, 0) AS idx_blks_read,
    r.idx_blks_hit + COALESCE(ch.idx_blks_hit, 0) AS idx_blks_hit
  FROM
    regular_indexes r
    LEFT JOIN chunk_sums ch
      USING (indexrelname);
END;
$$;

-- get_stat_statements
DROP FUNCTION IF EXISTS pganalyze.get_stat_statements;

CREATE FUNCTION pganalyze.get_stat_statements(job_id INT = NULL, config jsonb = NULL) RETURNS VOID
  LANGUAGE plpgsql AS
$$
DECLARE
  snapshot_time timestamptz := NOW();

BEGIN
  WITH statements AS (
    SELECT *
    FROM
      public.pg_stat_statements s
      JOIN pg_database d
        ON d.oid = s.dbid
    WHERE
      userid IN ('grafana'::regrole::oid, 'emhealth'::regrole::oid)
      AND queryid IS NOT NULL
      AND toplevel = 't'
      AND d.datname = CURRENT_DATABASE()
  ),

    queries
      AS ( INSERT INTO pganalyze.queries (queryid, query) SELECT queryid, query FROM statements ON CONFLICT DO NOTHING RETURNING queryid
    ),

    snapshot AS (
      INSERT INTO pganalyze.stat_snapshots
        SELECT
          snapshot_time AS collected_at,
          SUM(calls) AS calls,
          SUM(total_plan_time) AS total_plan_time,
          SUM(total_exec_time) AS total_exec_time,
          SUM(rows) AS rows,
          SUM(shared_blks_hit) AS shared_blks_hit,
          SUM(shared_blks_read) AS shared_blks_read,
          SUM(shared_blks_dirtied) AS shared_blks_dirtied,
          SUM(shared_blks_written) AS shared_blks_written,
          SUM(local_blks_hit) AS local_blks_hit,
          SUM(local_blks_read) AS local_blks_read,
          SUM(local_blks_dirtied) AS local_blks_dirtied,
          SUM(local_blks_written) AS local_blks_written,
          SUM(temp_blks_read) AS temp_blks_read,
          SUM(temp_blks_written) AS temp_blks_written,
          SUM(shared_blk_read_time) AS blk_read_time,
          SUM(shared_blk_write_time) AS blk_write_time,
          SUM(wal_records) AS wal_records,
          SUM(wal_fpi) AS wal_fpi,
          SUM(wal_bytes) AS wal_bytes,
          PG_POSTMASTER_START_TIME() AS stats_reset
        FROM
          statements
    )

  INSERT INTO pganalyze.stat_statements (
    collected_at,
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

CREATE FUNCTION pganalyze.parse_logs(job_id INT = NULL, config jsonb = NULL) RETURNS VOID
  LANGUAGE plpgsql AS
$$
DECLARE
  logfile TEXT;

BEGIN
  -- Construct a full path to a log file
  logfile := CURRENT_SETTING('data_directory') || '/' || (
    SELECT pg_current_logfile('csvlog')
  );
-- Create temp log table
  CREATE TEMP TABLE tmp_log (
    log_time TIMESTAMP(3) WITH TIME ZONE,
    user_name TEXT,
    database_name TEXT,
    process_id INTEGER,
    connection_from TEXT,
    session_id TEXT,
    session_line_num BIGINT,
    command_tag TEXT,
    session_start_time TIMESTAMP(3) WITH TIME ZONE,
    virtual_transaction_id TEXT,
    transaction_id BIGINT,
    error_severity TEXT,
    sql_state_code TEXT,
    message TEXT,
    detail TEXT,
    hint TEXT,
    internal_query TEXT,
    internal_query_pos INTEGER,
    context TEXT,
    query TEXT,
    query_pos INTEGER,
    location TEXT,
    application_name TEXT,
    backend_type TEXT,
    leader_pid INTEGER,
    query_id BIGINT,
    PRIMARY KEY (session_id, session_line_num)
  )
  ON COMMIT DROP;

-- Load CSV log data
  EXECUTE FORMAT('COPY tmp_log FROM %L WITH CSV', logfile);

-- Insert parsed vacuums into vacuum_stats
  WITH parsed AS (
    SELECT
      log_time,
      message,
      SUBSTRING(message FROM 'automatic vacuum of table "([^"]+)"') AS fqname,
      SUBSTRING(message FROM 'elapsed: ([0-9\.]+) s')::DOUBLE PRECISION AS elapsed_s,
      SUBSTRING(message FROM 'index scans: (\d+)')::BIGINT AS index_scans,
      SUBSTRING(message FROM 'pages: (\d+) removed')::BIGINT AS pages_removed,
      SUBSTRING(message FROM 'tuples: (\d+) removed')::BIGINT AS tuples_removed,
      SUBSTRING(message FROM 'tuples: \d+ removed, (\d+) remain')::INT AS tuples_remain,
      (message LIKE '%to prevent wraparound%') AS wraparound
    FROM
      tmp_log
    WHERE
      error_severity = 'LOG'
      AND backend_type = 'autovacuum worker'
      AND message LIKE 'automatic vacuum of table "%'
  ),
    split AS (
      SELECT *,
        SPLIT_PART(fqname, '.', 1) AS dbname,
        SPLIT_PART(fqname, '.', 2) AS schemaname,
        SPLIT_PART(fqname, '.', 3) AS tablename
      FROM
        parsed
      WHERE
        fqname IS NOT NULL
    ),
    filtered AS (
      SELECT *,
        schemaname || '.' || tablename AS relname
      FROM
        split
      WHERE
        dbname = CURRENT_DATABASE()
        AND (schemaname IN ('events', 'uec', 'pganalyze') OR
             (schemaname = '_timescaledb_internal' AND tablename LIKE '_hyper_%_chunk'))
    )
  INSERT
  INTO pganalyze.vacuum_stats (
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
    c.oid,
    log_time,
    log_time + (elapsed_s * INTERVAL '1 second'),
    index_scans,
    pages_removed,
    tuples_removed,
    tuples_remain,
    wraparound,
    message
  FROM
    filtered f
    JOIN pg_class c
      ON c.oid = TO_REGCLASS(f.relname)::oid
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
    SUBSTRING(message FROM 'duration: ([\d.]+) ms')::DOUBLE PRECISION,
    (SUBSTRING(message FROM 'plan:\n(\{.*)')::json #>> '{Plan,Total Cost}')::DOUBLE PRECISION AS total_cost,
    (SUBSTRING(message FROM 'plan:\n(\{.*)')::json #>> '{Plan,Shared Read Blocks}')::BIGINT * 8192 AS bytes_read,
    (SUBSTRING(message FROM 'plan:\n(\{.*)')::json #>> '{Plan,Shared I/O Read Time}')::DOUBLE PRECISION AS io_read_time,
    (SUBSTRING(message FROM 'plan:\n(\{.*)')::json #>> '{Plan}')::json AS plan
  FROM
    tmp_log
  WHERE
    database_name = CURRENT_DATABASE()
    AND user_name = 'grafana'
    AND error_severity = 'LOG'
    AND message LIKE 'duration: %'
  ON CONFLICT DO NOTHING;
END;
$$;

-- parse sysinfo
DROP FUNCTION IF EXISTS pganalyze.parse_sysinfo;

CREATE FUNCTION pganalyze.parse_sysinfo(job_id INT = NULL, config jsonb = NULL) RETURNS VOID
  LANGUAGE plpgsql AS
$$
BEGIN
  INSERT INTO pganalyze.sys_stats (
    load1,
    load5,
    load15,
    cpu_count,
    mem_total,
    mem_free,
    mem_avail
  )
  WITH loadavg AS (
    SELECT REGEXP_SPLIT_TO_ARRAY(PG_READ_FILE('/proc/loadavg', 0, 100), ' ') AS parts
  ),
    cpu AS (
      SELECT
        COUNT(*) AS cpu_count
      FROM
        UNNEST(STRING_TO_ARRAY(PG_READ_FILE('/proc/stat', 0, 2000), E'\n')) AS line
      WHERE
        line ~ '^cpu[0-9]+'
    ),
    mem AS (
      SELECT
        MAX(CASE WHEN line LIKE 'MemTotal:%'
                   THEN TRIM(REGEXP_REPLACE(SPLIT_PART(line, ':', 2), '[^0-9]', '', 'g'))::BIGINT END) AS mem_total,
        MAX(CASE WHEN line LIKE 'MemFree:%'
                   THEN TRIM(REGEXP_REPLACE(SPLIT_PART(line, ':', 2), '[^0-9]', '', 'g'))::BIGINT END) AS mem_free,
        MAX(CASE WHEN line LIKE 'MemAvailable:%'
                   THEN TRIM(REGEXP_REPLACE(SPLIT_PART(line, ':', 2), '[^0-9]', '', 'g'))::BIGINT END) AS mem_avail
      FROM
        (
          -- only read first 200 bytes of meminfo, which always covers first 3 lines
          SELECT UNNEST(STRING_TO_ARRAY(PG_READ_FILE('/proc/meminfo', 0, 200), E'\n')) AS line
        ) t
    )
  SELECT
    parts[1]::DOUBLE PRECISION AS load1,
    parts[2]::DOUBLE PRECISION AS load5,
    parts[3]::DOUBLE PRECISION AS load15,
    cpu_count,
    mem.mem_total,
    mem.mem_free,
    mem.mem_avail
  FROM
    loadavg,
    cpu,
    mem;
END;
$$;

-- Purge old data
DROP FUNCTION IF EXISTS pganalyze.purge_stats;

CREATE FUNCTION pganalyze.purge_stats(job_id INT = NULL, config jsonb = '{"drop_after":"3 months"}') RETURNS VOID
  LANGUAGE plpgsql AS
$$
DECLARE
  drop_after INTERVAL;

BEGIN
  SELECT jsonb_object_field_text(config, 'drop_after')::INTERVAL INTO STRICT drop_after;

  IF drop_after IS NULL THEN
    RAISE EXCEPTION 'Config must have drop_after';
  END IF;

  DELETE FROM pganalyze.database_stats WHERE collected_at < NOW() - drop_after;

  DELETE FROM pganalyze.table_stats WHERE collected_at < NOW() - drop_after;

  DELETE FROM pganalyze.index_stats WHERE collected_at < NOW() - drop_after;

  DELETE FROM pganalyze.vacuum_stats WHERE started_at < NOW() - drop_after;

  DELETE FROM pganalyze.stat_explains WHERE time < NOW() - drop_after;

  DELETE FROM pganalyze.stat_snapshots WHERE collected_at < NOW() - drop_after;

  DELETE FROM pganalyze.sys_stats WHERE time < NOW() - drop_after;

END;
$$;
