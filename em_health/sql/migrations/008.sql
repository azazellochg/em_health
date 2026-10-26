DO
$$
  DECLARE
    current_version INTEGER;
  BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 7 THEN
      -- 1. Run superuser jobs
      ALTER ROLE grafana CONNECTION LIMIT 100;
      ALTER ROLE emhealth CONNECTION LIMIT 100;

      -- 2. Refactor pganalyze.sys_stats and add sys_info
      SET ROLE pganalyze;
      ALTER TABLE pganalyze.stat_statements RENAME collected_at TO time;
      DROP TABLE IF EXISTS pganalyze.sys_stats;
      CREATE TABLE IF NOT EXISTS pganalyze.sys_stats (
        time timestamptz NOT NULL DEFAULT NOW(),
        metric TEXT NOT NULL,
        value DOUBLE PRECISION NOT NULL,
        labels JSONB,
        UNIQUE (metric, labels, time)
      )
      WITH (
        tsdb.hypertable,
        tsdb.chunk_interval = '7 days',
        tsdb.partition_column = 'time',
        tsdb.segmentby = 'metric',
        tsdb.orderby = 'time DESC'
        );

      PERFORM add_retention_policy('pganalyze.sys_stats', drop_after => INTERVAL '3 months');

      PERFORM delete_job(job_id)
      FROM timescaledb_information.jobs
      WHERE proc_name = 'parse_sysinfo';

      DROP FUNCTION IF EXISTS pganalyze.parse_sysinfo;
      CREATE TABLE IF NOT EXISTS pganalyze.sys_info (
        hostname TEXT PRIMARY KEY,
        os_name TEXT,
        kernel_name TEXT,
        kernel_version TEXT,
        cpu_count INTEGER NOT NULL
      );

      -- 3. Create new import funcs for pganalyze
      EXECUTE $sql$
CREATE OR REPLACE FUNCTION pganalyze.import_sysinfo(p_info_dict JSONB)
  RETURNS VOID
  LANGUAGE plpgsql AS
$func$
BEGIN
  INSERT INTO pganalyze.sys_info (
    hostname,
    os_name,
    kernel_name,
    kernel_version,
    cpu_count
  )
  VALUES (
    p_info_dict ->> 'hostname',
    p_info_dict ->> 'os_name',
    p_info_dict ->> 'kernel_name',
    p_info_dict ->> 'kernel_version',
    (p_info_dict ->> 'cpu_count')::INTEGER
  )
  ON CONFLICT (hostname) DO UPDATE SET
    os_name = excluded.os_name,
    kernel_name = excluded.kernel_name,
    kernel_version = excluded.kernel_version,
    cpu_count = excluded.cpu_count
  WHERE
    ROW (
      pganalyze.sys_info.os_name,
      pganalyze.sys_info.kernel_name,
      pganalyze.sys_info.kernel_version,
      pganalyze.sys_info.cpu_count
      )
      IS DISTINCT FROM ROW (
      excluded.os_name,
      excluded.kernel_name,
      excluded.kernel_version,
      excluded.cpu_count
      );
END;
$func$;

CREATE OR REPLACE FUNCTION pganalyze.import_sysstats(
  p_time timestamptz,
  p_stats_dict JSONB
)
  RETURNS VOID
  LANGUAGE plpgsql AS
$func$
BEGIN
  INSERT INTO pganalyze.sys_stats (
    time,
    metric,
    value,
    labels
  )
  SELECT
    p_time,
    item->>'metric',
    (item->>'value')::double precision,
    CASE
      WHEN item - 'metric' - 'value' = '{}'::jsonb
        THEN NULL
      ELSE item - 'metric' - 'value'
      END
  FROM jsonb_array_elements(p_stats_dict) AS item;
END;
$func$;

CREATE FUNCTION pganalyze.get_io_stats(job_id INT = NULL, config jsonb = NULL)
  RETURNS VOID
  LANGUAGE plpgsql AS
$func$
DECLARE
  snapshot_time timestamptz := NOW();

BEGIN
  INSERT INTO pganalyze.io_stats (
    collected_at,
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

      -- 4. Add new columns to pganalyze.database_stats
      ALTER TABLE pganalyze.database_stats
        ADD COLUMN wal_archived_count BIGINT NOT NULL DEFAULT 0,
        ADD COLUMN wal_failed_count BIGINT NOT NULL DEFAULT 0,
        ADD COLUMN chk_timed BIGINT NOT NULL DEFAULT 0,
        ADD COLUMN chk_requested BIGINT NOT NULL DEFAULT 0,
        ADD COLUMN chk_write_time DOUBLE PRECISION NOT NULL DEFAULT 0,
        ADD COLUMN chk_sync_time DOUBLE PRECISION NOT NULL DEFAULT 0,
        ADD COLUMN chk_buffers_written BIGINT NOT NULL DEFAULT 0;

      -- 5. New table and job pganalyze.io_stats
      CREATE TABLE IF NOT EXISTS pganalyze.io_stats (
        collected_at timestamptz NOT NULL,
        backend_type text NOT NULL,
        reads BIGINT,
        writes BIGINT,
        hits BIGINT,
        read_time DOUBLE PRECISION,
        write_time DOUBLE PRECISION,
        fsync_time DOUBLE PRECISION,
        fsyncs BIGINT,
        extends BIGINT,
        evictions BIGINT,
        reuses BIGINT,
        writebacks BIGINT,
        read_bytes BIGINT,
        write_bytes BIGINT,
        PRIMARY KEY (collected_at, backend_type)
      );

      SELECT add_job('pganalyze.get_io_stats', schedule_interval=>'1 min'::INTERVAL);

      -- 6. Add new column to pganalyze.table_stats
      ALTER TABLE pganalyze.table_stats
        ADD COLUMN seq_scans BIGINT NOT NULL DEFAULT 0;

      -- 7. Update schema version
      SET ROLE postgres;
      UPDATE public.schema_info SET version = 8;
      RAISE NOTICE 'Please run "emhealth dev -d tem create-stats -f" and "emhealth dev -d sem create-stats -f"';
    END IF;
  END
$$
