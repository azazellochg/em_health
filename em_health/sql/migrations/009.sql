DO
$$
  DECLARE
    current_version INTEGER;
    r RECORD;
    v_view TEXT;
  BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 8 THEN
      -- 1. One shared refresh procedure for materialized views, replaces generated <schema>.refresh_<view>
      EXECUTE $sql$
CREATE OR REPLACE PROCEDURE public.refresh_mview(job_id INT, config JSONB)
  LANGUAGE plpgsql AS
$func$
DECLARE
  v_view REGCLASS := (config ->> 'view')::REGCLASS;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = v_view AND relkind = 'm') THEN
    RAISE EXCEPTION 'public.refresh_mview: % is not a materialized view', config ->> 'view';
  END IF;
  EXECUTE format('REFRESH MATERIALIZED VIEW %s', v_view);
END;
$func$;
$sql$;

      REVOKE ALL ON PROCEDURE public.refresh_mview(INT, JSONB) FROM PUBLIC;
      GRANT EXECUTE ON PROCEDURE public.refresh_mview(INT, JSONB) TO emhealth, pganalyze;

      -- 2. Move existing refresh jobs to public.refresh_mview, keeping their owner and interval
      FOR r IN
        SELECT job_id, proc_schema, proc_name, schedule_interval, owner::text AS owner
        FROM timescaledb_information.jobs
        WHERE proc_schema IN ('events', 'pganalyze')
          AND proc_name LIKE 'refresh\_%'
      LOOP
        v_view := r.proc_schema || '.' || substr(r.proc_name, length('refresh_') + 1);
        PERFORM delete_job(r.job_id);
        IF to_regclass(v_view) IS NOT NULL THEN
          EXECUTE format('SET ROLE %I', r.owner);
          PERFORM add_job('public.refresh_mview', r.schedule_interval,
                          config => jsonb_build_object('view', v_view));
          SET ROLE postgres;
        END IF;
      END LOOP;

      -- 3. Drop the old generated refresh procedures
      FOR r IN
        SELECT n.nspname, p.proname
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname IN ('events', 'pganalyze')
          AND p.proname LIKE 'refresh\_%'
          AND p.prokind = 'p'
      LOOP
        EXECUTE format('DROP PROCEDURE IF EXISTS %I.%I', r.nspname, r.proname);
      END LOOP;

      -- 4. Static UEC import procedure, replaces generated uec.import_from_<server> functions
      SET ROLE emhealth;
      EXECUTE $sql$
CREATE OR REPLACE PROCEDURE uec.import_errors(job_id INT, config JSONB)
  LANGUAGE plpgsql AS
$func$
DECLARE
  v_schema TEXT := config ->> 'fdw_schema';
  v_instrument_id BIGINT := (config ->> 'instrument_id')::BIGINT;
BEGIN
  IF v_schema IS NULL OR v_instrument_id IS NULL THEN
    RAISE EXCEPTION 'uec.import_errors: config needs fdw_schema and instrument_id, got %', config;
  END IF;

  -- Get new error definitions
  DROP TABLE IF EXISTS pg_temp.new_error_types;
  EXECUTE format($q$
    CREATE TEMP TABLE new_error_types ON COMMIT DROP AS
    SELECT *
    FROM %I.error_definitions edf
    WHERE edf.ErrorDefinitionID > COALESCE((SELECT MAX(error_definition_id) FROM uec.error_definitions), 0)
  $q$, v_schema);

  -- Subsystems
  INSERT INTO uec.subsystem (subsystem_id, name)
  SELECT DISTINCT SubsystemID, Subsystem
  FROM new_error_types
  ON CONFLICT (subsystem_id) DO NOTHING;

  -- Device Types
  INSERT INTO uec.device_type (device_type_id, name)
  SELECT DISTINCT DeviceTypeID, DeviceType
  FROM new_error_types
  ON CONFLICT (device_type_id) DO NOTHING;

  -- Device Instances
  INSERT INTO uec.device_instance (device_instance_id, device_type_id, name)
  SELECT DISTINCT DeviceInstanceID, DeviceTypeID, DeviceInstance
  FROM new_error_types
  ON CONFLICT (device_instance_id, device_type_id) DO NOTHING;

  -- Error Codes
  INSERT INTO uec.error_code (device_type_id, error_code_id, name)
  SELECT DISTINCT DeviceTypeID, ErrorCodeID, ErrorCode
  FROM new_error_types
  ON CONFLICT (device_type_id, error_code_id) DO NOTHING;

  -- Error definitions
  INSERT INTO uec.error_definitions (
    error_definition_id,
    subsystem_id,
    device_type_id,
    error_code_id,
    device_instance_id
  )
  SELECT
    n.ErrorDefinitionID,
    n.SubsystemID,
    n.DeviceTypeID,
    n.ErrorCodeID,
    n.DeviceInstanceID
  FROM new_error_types n
  ON CONFLICT (error_definition_id) DO NOTHING;

  -- Error notifications
  EXECUTE format($q$
    INSERT INTO uec.errors (time, instrument_id, error_id, message)
    SELECT
      en.ErrorDtm,
      $1,
      ed.error_definition_id,
      en.MessageText
    FROM %I.error_notifications en
    JOIN uec.error_definitions ed ON ed.error_definition_id = en.ErrorDefinitionID
    WHERE en.ErrorDtm > (SELECT MAX(time) FROM uec.errors WHERE instrument_id = $1)
    ON CONFLICT (time, instrument_id, error_id) DO NOTHING
  $q$, v_schema) USING v_instrument_id;
END;
$func$;
$sql$;

      REVOKE ALL ON PROCEDURE uec.import_errors(INT, JSONB) FROM PUBLIC;
      SET ROLE postgres;

      -- 5. Remove the old generated UEC import jobs and functions.
      --    Run 'emhealth dev -d tem import-uec' again to schedule uec.import_errors.
      PERFORM delete_job(job_id)
      FROM timescaledb_information.jobs
      WHERE proc_schema = 'uec'
        AND proc_name LIKE 'import\_from\_%';

      FOR r IN
        SELECT p.proname
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'uec'
          AND p.proname LIKE 'import\_from\_%'
      LOOP
        EXECUTE format('DROP FUNCTION IF EXISTS uec.%I', r.proname);
      END LOOP;

      -- 6. Update schema version
      UPDATE public.schema_info SET version = 9, updated = NOW();
    END IF;
  END
$$
