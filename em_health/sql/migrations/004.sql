DO
$$
  DECLARE
    current_version INTEGER;
  BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 3 THEN
      -- 1. Remove unused column from index_stats
      ALTER TABLE pganalyze.index_stats
        DROP COLUMN exclusively_locked;

      -- 2. Increase chunk size for main table
      PERFORM set_chunk_time_interval('public.data', INTERVAL '7 days');

      -- 3. Rename uec errors column
      ALTER TABLE uec.errors
        RENAME COLUMN instrumentid TO instrument_id;
      ALTER TABLE uec.errors
        DROP CONSTRAINT errors_time_instrumentid_errorid_key;
      ALTER TABLE uec.errors
        ADD CONSTRAINT errors_time_instrument_id_errorid_key UNIQUE (time, instrument_id, errorid);

      -- 4. Add a new column to instruments
      ALTER TABLE public.instruments
        ADD COLUMN
          is_active BOOLEAN DEFAULT TRUE;

      -- 5. Add index to sys_stats
      ALTER TABLE pganalyze.sys_stats
        ADD CONSTRAINT sys_stats_time_key UNIQUE (time);

      -- 6. Reorder index to match the queries
      ALTER TABLE pganalyze.stat_explains
        DROP CONSTRAINT stat_explains_pkey;
      ALTER TABLE pganalyze.stat_explains
        ADD PRIMARY KEY (queryid, time);

      -- 7. Move wal_position column
      ALTER TABLE pganalyze.stat_snapshots
        DROP COLUMN wal_position;
      ALTER TABLE pganalyze.database_stats
        ADD COLUMN
          wal_lsn pg_lsn NOT NULL DEFAULT '0/0';

      -- 8. Update schema version
      UPDATE public.schema_info SET version = 4;
    END IF;
  END
$$
