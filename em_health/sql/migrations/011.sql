DO
$$
  DECLARE
    current_version INTEGER;
    v_chunk_interval INTERVAL;
    v_compress_after INTERVAL;
    v_retention INTERVAL;
  BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 10 THEN
      SET ROLE pganalyze;

      -- Migrations get no .env variables: reuse the sys_stats settings, which came from
      -- TBL_STATS_CHUNK_SIZE and TBL_STATS_RETENTION
      SELECT time_interval INTO v_chunk_interval
      FROM timescaledb_information.dimensions
      WHERE hypertable_schema = 'pganalyze' AND hypertable_name = 'sys_stats';

      SELECT (config ->> 'compress_after')::INTERVAL INTO v_compress_after
      FROM timescaledb_information.jobs
      WHERE hypertable_schema = 'pganalyze' AND hypertable_name = 'sys_stats'
        AND proc_name = 'policy_compression';

      SELECT (config ->> 'drop_after')::INTERVAL INTO v_retention
      FROM timescaledb_information.jobs
      WHERE hypertable_schema = 'pganalyze' AND hypertable_name = 'sys_stats'
        AND proc_name = 'policy_retention';

      v_chunk_interval := COALESCE(v_chunk_interval, INTERVAL '7 days');
      v_compress_after := COALESCE(v_compress_after, v_chunk_interval);
      v_retention := COALESCE(v_retention, INTERVAL '3 months');

      -- 1. Use the same time column name as the other pganalyze tables (stat_statements, sys_stats)
      ALTER TABLE pganalyze.database_stats RENAME COLUMN collected_at TO time;
      ALTER TABLE pganalyze.table_stats RENAME COLUMN collected_at TO time;
      ALTER TABLE pganalyze.index_stats RENAME COLUMN collected_at TO time;
      ALTER TABLE pganalyze.io_stats RENAME COLUMN collected_at TO time;

      -- A rename keeps the old column name inside existing indexes, and create_hypertable checks
      -- the primary key by those names. Rebuild the primary keys (small tables)
      ALTER TABLE pganalyze.database_stats DROP CONSTRAINT database_stats_pkey, ADD PRIMARY KEY (time);
      ALTER TABLE pganalyze.table_stats DROP CONSTRAINT table_stats_pkey, ADD PRIMARY KEY (relid, time);
      ALTER TABLE pganalyze.index_stats DROP CONSTRAINT index_stats_pkey, ADD PRIMARY KEY (indexrelid, time);
      ALTER TABLE pganalyze.io_stats DROP CONSTRAINT io_stats_pkey, ADD PRIMARY KEY (time, backend_type);

      -- The collector functions use the renamed columns
      EXECUTE $sql$
CREATE OR REPLACE FUNCTION pganalyze.get_db_stats(job_id INT = NULL, config jsonb = NULL) RETURNS VOID
  LANGUAGE plpgsql AS
$func$
BEGIN
  INSERT INTO pganalyze.database_stats (
    time,
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
    wal_lsn,
    xmin_horizon,
    wal_archived_count,
    wal_failed_count,
    chk_timed,
    chk_requested,
    chk_write_time,
    chk_sync_time,
    chk_buffers_written
  )
  WITH xmin_candidates AS (
    SELECT
      (
        SELECT
          CASE
            WHEN COALESCE(pg_catalog.age(backend_xid), 0) > COALESCE(pg_catalog.age(backend_xmin), 0)
              THEN backend_xid::text::bigint
            ELSE backend_xmin::text::bigint
            END
        FROM pg_catalog.pg_stat_activity
        WHERE backend_xmin IS NOT NULL
          OR backend_xid IS NOT NULL
        ORDER BY greatest(pg_catalog.age(backend_xmin), pg_catalog.age(backend_xid)) DESC
        LIMIT 1
      ) AS backend,
      (
        SELECT transaction::text::bigint
        FROM pg_catalog.pg_prepared_xacts
        ORDER BY pg_catalog.age(transaction) DESC
        LIMIT 1
      ) AS prepare_xact
  ),
    true_xmin AS (
      SELECT
        COALESCE(LEAST(backend, prepare_xact), 0) AS xmin_horizon
      FROM xmin_candidates
    )
  SELECT
    NOW() AS time,
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
    pg_current_wal_lsn() AS wal_lsn,
    tx.xmin_horizon,
    a.archived_count AS wal_archived_count,
    a.failed_count AS wal_failed_count,
    c.num_timed AS chk_timed,
    c.num_requested AS chk_requested,
    c.write_time AS chk_write_time,
    c.sync_time AS chk_sync_time,
    c.buffers_written AS chk_buffers_written
  FROM
    pg_catalog.pg_stat_database s
    JOIN pg_catalog.pg_database d
      ON s.datname = d.datname
    CROSS JOIN true_xmin tx
    CROSS JOIN pg_stat_archiver a
    CROSS JOIN pg_stat_checkpointer c
  WHERE
    s.datname = CURRENT_DATABASE();
END;
$func$;
$sql$;

      EXECUTE $sql$
CREATE OR REPLACE FUNCTION pganalyze.get_table_stats(job_id INT = NULL, config jsonb = NULL) RETURNS VOID
  LANGUAGE plpgsql AS
$func$
BEGIN
  WITH chunk_stats AS (
    SELECT
      ch.hypertable_schema,
      ch.hypertable_name,
      MAX(AGE(c.relfrozenxid)) AS frozen_xid_age,
      COALESCE(SUM(s.n_tup_ins), 0) AS tup_ins,
      COALESCE(SUM(s.n_tup_upd), 0) AS tup_upd,
      COALESCE(SUM(s.n_tup_del), 0) AS tup_del,
      COALESCE(SUM(s.n_dead_tup), 0) AS num_dead_rows,
      COALESCE(SUM(s.n_live_tup), 0) AS num_live_rows,
      COALESCE(SUM(s.seq_scan), 0) AS seq_scans
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
    time,
    relid,
    table_bytes,
    index_bytes,
    toast_bytes,
    frozen_xid_age,
    tup_ins,
    tup_upd,
    tup_del,
    num_dead_rows,
    num_live_rows,
    seq_scans
  )
  SELECT
    NOW() AS time,
    s.relid,
    CASE WHEN ht.hypertable_name IS NOT NULL THEN (hds).table_bytes ELSE PG_TABLE_SIZE(s.relid) END AS table_bytes,
    CASE WHEN ht.hypertable_name IS NOT NULL THEN (hds).index_bytes ELSE PG_INDEXES_SIZE(s.relid) END AS index_bytes,
    CASE WHEN ht.hypertable_name IS NOT NULL THEN (hds).toast_bytes
         ELSE PG_TOTAL_RELATION_SIZE(s.relid) - PG_TABLE_SIZE(s.relid) -
              PG_INDEXES_SIZE(s.relid) END AS toast_external_bytes,
    CASE WHEN ht.hypertable_name IS NOT NULL THEN COALESCE(cs.frozen_xid_age, 0) ELSE COALESCE(AGE(c.relfrozenxid), 0) END AS frozen_xid_age,
    CASE WHEN ht.hypertable_name IS NOT NULL THEN COALESCE(cs.tup_ins, 0) ELSE COALESCE(st.n_tup_ins, 0) END AS tup_ins,
    CASE WHEN ht.hypertable_name IS NOT NULL THEN COALESCE(cs.tup_upd, 0) ELSE COALESCE(st.n_tup_upd, 0) END AS tup_upd,
    CASE WHEN ht.hypertable_name IS NOT NULL THEN COALESCE(cs.tup_del, 0) ELSE COALESCE(st.n_tup_del, 0) END AS tup_del,
    CASE WHEN ht.hypertable_name IS NOT NULL THEN COALESCE(cs.num_dead_rows, 0) ELSE COALESCE(st.n_dead_tup, 0) END AS num_dead_rows,
    CASE WHEN ht.hypertable_name IS NOT NULL THEN COALESCE(cs.num_live_rows, 0) ELSE COALESCE(st.n_live_tup, 0) END AS num_live_rows,
    CASE WHEN ht.hypertable_name IS NOT NULL THEN COALESCE(cs.seq_scans, 0) ELSE COALESCE(st.seq_scan, 0) END AS seq_scans

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
$func$;
$sql$;

      EXECUTE $sql$
CREATE OR REPLACE FUNCTION pganalyze.get_index_stats(job_id INT = NULL, config jsonb = NULL) RETURNS VOID
  LANGUAGE plpgsql AS
$func$
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
        REGEXP_REPLACE(s.indexrelname, '^(.*?_chunk_|(?:[0-9]+_)+)', '') AS indexrelname, -- match pkey, unique and normal indexes on chunks
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
    time,
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
    NOW() AS time,
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
$func$;
$sql$;

      EXECUTE $sql$
CREATE OR REPLACE FUNCTION pganalyze.get_io_stats(job_id INT = NULL, config jsonb = NULL) RETURNS VOID
  LANGUAGE plpgsql AS
$func$
DECLARE
  snapshot_time timestamptz := NOW();

BEGIN
  INSERT INTO pganalyze.io_stats (
    time,
    backend_type,
    reads,
    writes,
    hits,
    read_time,
    write_time,
    fsync_time,
    fsyncs,
    extends,
    evictions,
    reuses,
    writebacks,
    read_bytes,
    write_bytes
  )
  SELECT
    snapshot_time,
    backend_type,
    SUM(reads) AS reads,
    SUM(writes) AS writes,
    SUM(hits) AS hits,
    SUM(read_time) AS read_time,
    SUM(write_time) AS write_time,
    SUM(fsync_time) AS fsync_time,
    SUM(fsyncs) AS fsyncs,
    SUM(extends) AS extends,
    SUM(evictions) AS evictions,
    SUM(reuses) AS reuses,
    SUM(writebacks) AS writebacks,
    SUM(read_bytes) AS read_bytes,
    SUM(write_bytes) AS write_bytes
  FROM pg_stat_io
  GROUP BY backend_type;
END;
$func$;
$sql$;

      -- 2. io_stats was never purged: make it a hypertable with compression and retention
      PERFORM create_hypertable('pganalyze.io_stats',
                                by_range('time', v_chunk_interval),
                                create_default_indexes => FALSE,
                                migrate_data => TRUE);

      ALTER TABLE pganalyze.io_stats SET (
        timescaledb.enable_columnstore,
        timescaledb.segmentby = 'backend_type',
        timescaledb.orderby = 'time DESC'
        );

      CALL add_columnstore_policy('pganalyze.io_stats', after => v_compress_after);
      PERFORM add_retention_policy('pganalyze.io_stats', drop_after => v_retention);

      -- 3. Hypertables are purged by retention policies only. Deleting from sys_stats as well
      -- only decompressed rows of the oldest (compressed) chunk every day
      EXECUTE $sql$
CREATE OR REPLACE FUNCTION pganalyze.purge_stats(job_id INT = NULL, config jsonb = '{"drop_after":"3 months"}') RETURNS VOID
  LANGUAGE plpgsql AS
$func$
DECLARE
  drop_after INTERVAL;

BEGIN
  SELECT jsonb_object_field_text(config, 'drop_after')::INTERVAL INTO STRICT drop_after;

  IF drop_after IS NULL THEN
    RAISE EXCEPTION 'Config must have drop_after';
  END IF;

  DELETE FROM pganalyze.database_stats WHERE time < NOW() - drop_after;

  DELETE FROM pganalyze.table_stats WHERE time < NOW() - drop_after;

  DELETE FROM pganalyze.index_stats WHERE time < NOW() - drop_after;

  DELETE FROM pganalyze.vacuum_stats WHERE started_at < NOW() - drop_after;

  DELETE FROM pganalyze.stat_explains WHERE time < NOW() - drop_after;

  -- Hypertables (stat_statements, sys_stats, io_stats) are purged by their retention policies
END;
$func$;
$sql$;

      -- 4. sys_stats: the unique constraint was larger than the table, did not cover rows with
      -- NULL labels and nothing relied on it. Queries filter on metric and a time range
      ALTER TABLE pganalyze.sys_stats DROP CONSTRAINT IF EXISTS sys_stats_metric_labels_time_key;
      DROP INDEX IF EXISTS pganalyze.sys_stats_time_idx;
      CREATE INDEX IF NOT EXISTS sys_stats_metric_time_idx ON pganalyze.sys_stats (metric, time DESC);

      -- 5. purge_stats keeps plain tables as long as the hypertable retention policies
      PERFORM alter_job(job_id, config => jsonb_build_object('drop_after', v_retention))
      FROM timescaledb_information.jobs
      WHERE proc_schema = 'pganalyze' AND proc_name = 'purge_stats';

      -- 6. Update schema version
      SET ROLE postgres;
      UPDATE public.schema_info SET version = 11, updated = NOW();
    END IF;
  END
$$
