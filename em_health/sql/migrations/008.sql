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

      SELECT add_retention_policy('pganalyze.sys_stats', drop_after => INTERVAL '3 months');

      SELECT delete_job(job_id)
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
$sql$;

      -- 4. Update schema version
      UPDATE public.schema_info SET version = 8;
    END IF;
  END
$$
