DO
$$
  DECLARE
    current_version INTEGER;
    r RECORD;
  BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 14 THEN
      -- 1. Drop em_off_daily cagg
      DROP MATERIALIZED VIEW IF EXISTS events.em_off_daily CASCADE;

      -- 2. Drop epud_runs (EPU-D has no runs in the data) and its refresh job
      PERFORM delete_job(job_id)
      FROM timescaledb_information.jobs
      WHERE proc_schema = 'public'
        AND proc_name = 'refresh_mview'
        AND config ->> 'view' = 'events.epud_runs';
      DROP MATERIALIZED VIEW IF EXISTS events.epud_runs;

      -- 3. Update schema version
      UPDATE public.schema_info SET version = 15, updated = NOW();
    END IF;
  END
$$
