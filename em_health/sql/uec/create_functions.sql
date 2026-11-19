-- Import new UEC error definitions and notifications for one instrument from its foreign tables
-- fdw.ms_<id>_error_definitions and fdw.ms_<id>_error_notifications (created by FDWManager).
-- Scheduled per instrument by DatabaseManager.schedule_uec_import() with config = {"instrument_id": <id>}.
-- The table names are built from the integer id and quoted with format('%I'), the id is a bound parameter.
CREATE OR REPLACE PROCEDURE uec.import_errors(job_id INT, config JSONB)
  LANGUAGE plpgsql AS
$func$
DECLARE
  v_instrument_id BIGINT := (config ->> 'instrument_id')::BIGINT;
  v_definitions TEXT := format('ms_%s_error_definitions', v_instrument_id);
  v_notifications TEXT := format('ms_%s_error_notifications', v_instrument_id);
BEGIN
  IF v_instrument_id IS NULL THEN
    RAISE EXCEPTION 'uec.import_errors: config needs instrument_id, got %', config;
  END IF;
  IF to_regclass(format('fdw.%I', v_notifications)) IS NULL THEN
    RAISE EXCEPTION 'uec.import_errors: foreign table fdw.% not found, run import-uec', v_notifications;
  END IF;

  -- Get new error definitions
  DROP TABLE IF EXISTS pg_temp.new_error_types;
  EXECUTE format($q$
    CREATE TEMP TABLE new_error_types ON COMMIT DROP AS
    SELECT *
    FROM fdw.%I edf
    -- -1 on the first run: the UEC catalogue starts at ErrorDefinitionID 0
    WHERE edf.ErrorDefinitionID > COALESCE((SELECT MAX(error_definition_id) FROM uec.error_definitions), -1)
  $q$, v_definitions);

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
    FROM fdw.%I en
    JOIN uec.error_definitions ed ON ed.error_definition_id = en.ErrorDefinitionID
    -- '-infinity' on the first run, when the instrument has no errors yet (MAX is NULL)
    WHERE en.ErrorDtm > COALESCE((SELECT MAX(time) FROM uec.errors WHERE instrument_id = $1), '-infinity')
    ON CONFLICT (time, instrument_id, error_id) DO NOTHING
  $q$, v_notifications) USING v_instrument_id;
END;
$func$;

REVOKE ALL ON PROCEDURE uec.import_errors(INT, JSONB) FROM PUBLIC;
