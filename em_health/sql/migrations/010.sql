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

      -- 2. Fix typo in the function
      SET ROLE emhealth;
      EXECUTE $sql$
CREATE OR REPLACE FUNCTION events.purge_old_chunks(
  p_hypertable_name TEXT,
  p_retain_days INT,
  OUT chunks_dropped INT
)
  LANGUAGE plpgsql AS
$func$
DECLARE
  v_interval INTERVAL;
BEGIN
  chunks_dropped := 0;

  IF p_retain_days <= 0 THEN
    RETURN;
  END IF;

  v_interval := p_retain_days * INTERVAL '1 day';

  SELECT COUNT(*) INTO chunks_dropped FROM drop_chunks(p_hypertable_name, older_than => v_interval);

  RAISE NOTICE '% chunks older than % days dropped from %', chunks_dropped, p_retain_days, p_hypertable_name;

  RETURN;
END;
$func$;
$sql$;

      -- 3. Update schema version
      UPDATE public.schema_info SET version = 10, updated = NOW();
    END IF;
  END
$$
