DO
$$
  DECLARE
    current_version INTEGER;
    job INTEGER;
    chunk REGCLASS;
  BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 5 THEN
      -- 1. Change segment_by for events/data. (instrument_id,param_id) gives <100 rows/chunk so it's way too granular
      SELECT
        job_id
      INTO job
      FROM
        timescaledb_information.jobs
      WHERE
        proc_name = 'policy_compression'
        AND hypertable_schema = 'events'
        AND hypertable_name = 'data';

      PERFORM alter_job(job, scheduled => FALSE);

      -- this will take quite some time on a large events.data table
      FOR chunk IN SELECT show_chunks('events.data') LOOP
        RAISE NOTICE 'Converting % to rowstore', chunk;
        CALL convert_to_rowstore(chunk);
      END LOOP;

      ALTER TABLE events.data
        SET (
          timescaledb.segmentby = 'instrument_id',
          timescaledb.orderby = 'param_id, time DESC'
          );

      PERFORM alter_job(job, scheduled => TRUE);

      -- 2. Update pganalyze.purge_stats func
      PERFORM delete_job(job_id) FROM timescaledb_information.jobs WHERE proc_name = 'purge_stats';
      EXECUTE $sql$
DROP FUNCTION IF EXISTS pganalyze.purge_stats;
CREATE FUNCTION pganalyze.purge_stats(job_id int = NULL, config jsonb = '{"drop_after":"3 months"}')
RETURNS void
LANGUAGE plpgsql
AS $func$
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
$func$;
$sql$;

      PERFORM add_job('pganalyze.purge_stats', schedule_interval=>'1 day'::INTERVAL,
                      config => '{"drop_after":"3 months"}');

      SET ROLE postgres;

      -- 3. Fix constraint for events.enum_values
      ALTER TABLE events.enum_values
        DROP CONSTRAINT enum_values_enum_id_member_name_value_key;
      ALTER TABLE events.enum_values
        ADD CONSTRAINT enum_values_enum_id_member_name_key UNIQUE (enum_id, member_name),
        ADD CONSTRAINT enum_values_enum_id_value_key UNIQUE (enum_id, value);

      -- 4. Drop bad indexes
      DROP INDEX events.enum_values_member_name_enum_id_idx;
      DROP INDEX events.parameters_enum_id_instrument_id_param_id_param_name_subsys_idx;

      -- 5. Fix SET NULL condition for events.parameters FK
      ALTER TABLE events.parameters
        DROP CONSTRAINT parameters_enum_id_instrument_id_fkey;
      ALTER TABLE events.parameters
        ADD CONSTRAINT parameters_enum_id_instrument_id_fkey FOREIGN KEY (enum_id, instrument_id) REFERENCES events.enum_types (id, instrument_id) ON DELETE SET NULL (enum_id);

      ALTER TABLE events.parameters_history
        DROP CONSTRAINT parameters_history_enum_id_instrument_id_fkey;
      ALTER TABLE events.parameters_history
        ADD CONSTRAINT parameters_history_enum_id_instrument_id_fkey FOREIGN KEY (enum_id, instrument_id) REFERENCES events.enum_types (id, instrument_id) ON DELETE SET NULL (enum_id);

      -- 6. Make the staging table unlogged
      ALTER TABLE events.data_staging
        SET UNLOGGED;

      -- 7. Add new columns to pganalyze tables
      ALTER TABLE pganalyze.database_stats
        ADD COLUMN xmin_horizon BIGINT NOT NULL DEFAULT 0;

      ALTER TABLE pganalyze.table_stats
        ADD COLUMN IF NOT EXISTS tup_ins BIGINT NOT NULL DEFAULT 0,
        ADD COLUMN IF NOT EXISTS tup_upd BIGINT NOT NULL DEFAULT 0,
        ADD COLUMN IF NOT EXISTS tup_del BIGINT NOT NULL DEFAULT 0;

      ALTER TABLE pganalyze.vacuum_stats
        ADD COLUMN IF NOT EXISTS hypertable_relid oid;

      -- 8. Update hypertable_relid in pganalyze.vacuum_stats for the old rows
      -- update values for normal tables
      UPDATE pganalyze.vacuum_stats
      SET hypertable_relid = relid
      WHERE hypertable_relid IS NULL
        AND EXISTS (
          SELECT 1
          FROM pg_class c
          WHERE c.oid = pganalyze.vacuum_stats.relid
        )
        AND NOT EXISTS (
          SELECT 1
          FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
               JOIN timescaledb_information.chunks ch
              ON ch.chunk_schema = n.nspname
              AND ch.chunk_name = c.relname
          WHERE c.oid = pganalyze.vacuum_stats.relid
        );

      -- update values for hypertable chunks
      UPDATE pganalyze.vacuum_stats v
      SET hypertable_relid = h.oid
      FROM pg_class c
           JOIN pg_namespace n
          ON n.oid = c.relnamespace
           JOIN timescaledb_information.chunks ch
          ON ch.chunk_schema = n.nspname
          AND ch.chunk_name = c.relname
           JOIN pg_namespace hn
          ON hn.nspname = ch.hypertable_schema
           JOIN pg_class h
          ON h.relnamespace = hn.oid
          AND h.relname = ch.hypertable_name
      WHERE v.hypertable_relid IS NULL
        AND v.relid = c.oid;

      -- update remaining values (mostly dropped chunks)
      UPDATE pganalyze.vacuum_stats
      SET hypertable_relid = relid
      WHERE hypertable_relid IS NULL;

      -- set the column NOT NULL
      ALTER TABLE pganalyze.vacuum_stats
        ALTER COLUMN hypertable_relid SET NOT NULL;

      -- 9. Drop pganalyze.stat_snapshots
      DROP TABLE pganalyze.stat_snapshots;

      -- 10. Create a new extension for future index checks
      CREATE EXTENSION IF NOT EXISTS amcheck;

      -- 11. Update schema version
      UPDATE public.schema_info SET version = 6;
    END IF;
  END
$$
