DO
$$
  DECLARE
    current_version INTEGER;
  BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 11 THEN
      SET ROLE pganalyze;

      -- 1. Transaction ID use is measured from the transaction ID counters. xact_commit + xact_rollback
      -- also counts read-only transactions, which get no transaction ID
      ALTER TABLE pganalyze.database_stats
        ADD COLUMN IF NOT EXISTS next_xid BIGINT,
        ADD COLUMN IF NOT EXISTS next_mxid BIGINT;

      -- 2. The collector fills the new columns
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
    chk_buffers_written,
    next_xid,
    next_mxid
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
    c.buffers_written AS chk_buffers_written,
    -- read from the snapshot: pg_current_xact_id() would use up a transaction ID
    pg_snapshot_xmax(pg_current_snapshot())::text::bigint AS next_xid,
    -- mxid_age() counts from the next multixact ID
    (d.datminmxid::text::bigint + mxid_age(d.datminmxid)) % 4294967296 AS next_mxid
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

      -- 3. Update schema version
      SET ROLE postgres;
      UPDATE public.schema_info SET version = 12, updated = NOW();
    END IF;
  END
$$
