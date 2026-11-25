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


      -- 2. Update schema version
      UPDATE public.schema_info SET version = 15, updated = NOW();
    END IF;
  END
$$
