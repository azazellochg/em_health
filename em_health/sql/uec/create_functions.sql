-- Import new UEC error definitions and notifications for one instrument from its FDW schema.
-- Scheduled per instrument by DatabaseManager.schedule_uec_import() with
--   config = {"instrument_id": <id>, "fdw_schema": "fdw_ms_<id>"}
-- The schema name is only used through format('%I'), the instrument id is a bound parameter.
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

REVOKE ALL ON PROCEDURE uec.import_errors(INT, JSONB) FROM PUBLIC;
