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

      -- 2. Update schema version
      UPDATE public.schema_info SET version = 8;
    END IF;
  END
$$
