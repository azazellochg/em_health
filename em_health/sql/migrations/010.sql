DO
$$
  DECLARE
    current_version INTEGER;
    r RECORD;
    v_view TEXT;
  BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 9 THEN
      -- 1. Change config values
      ALTER ROLE grafana CONNECTION LIMIT 50;
      ALTER ROLE emhealth CONNECTION LIMIT 50;
      ALTER ROLE grafana SET statement_timeout = '5min';

      -- 2. Update schema version
      UPDATE public.schema_info SET version = 10, updated = NOW();
    END IF;
  END
$$
