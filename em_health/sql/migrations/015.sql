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

      -- 3. Drop epu/tomo_state_daily caggs: the running_daily views are built from the sessions now.
      -- CASCADE drops the old running_daily views, delete their refresh jobs (create-stats adds them again)
      PERFORM delete_job(job_id)
      FROM timescaledb_information.jobs
      WHERE proc_schema = 'public'
        AND proc_name = 'refresh_mview'
        AND config ->> 'view' IN ('events.epu_running_daily', 'events.tomo_running_daily');
      DROP MATERIALIZED VIEW IF EXISTS events.epu_state_daily CASCADE;
      DROP MATERIALIZED VIEW IF EXISTS events.tomo_state_daily CASCADE;

      -- 4. Drop tem_cryocycle_daily cagg: replaced by the tem_cryocycles mview (create-stats)
      DROP MATERIALIZED VIEW IF EXISTS events.tem_cryocycle_daily;

      -- 5. Drop sem_cryocycle_al_daily and sem_cryocycle_noal_daily caggs: sem_cryocycles reads the raw data now
      -- (autoloader systems only). CASCADE drops sem_cryocycles and sem_beamtime_daily, delete their refresh jobs
      -- (create-stats adds them again)
      PERFORM delete_job(job_id)
      FROM timescaledb_information.jobs
      WHERE proc_schema = 'public'
        AND proc_name = 'refresh_mview'
        AND config ->> 'view' IN ('events.sem_cryocycles', 'events.sem_beamtime_daily');
      DROP MATERIALIZED VIEW IF EXISTS events.sem_cryocycle_al_daily CASCADE;
      DROP MATERIALIZED VIEW IF EXISTS events.sem_cryocycle_noal_daily CASCADE;

      -- 6. Drop sem_chamber_state_daily cagg: sem_beamtime_daily reads the chamber state from raw data now
      -- (sem_beamtime_daily is dropped and its refresh job deleted in step 5)
      DROP MATERIALIZED VIEW IF EXISTS events.sem_chamber_state_daily CASCADE;

      -- 7. Update schema version
      UPDATE public.schema_info SET version = 15, updated = NOW();
    END IF;
  END
$$
