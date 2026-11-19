DO
$$
  DECLARE
    current_version INTEGER;
  BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 12 THEN
      SET ROLE emhealth;

      -- 1. The uec columns were renamed in the base DDL (e.g. errorid -> error_id, identifyingname -> name)
      --    without a migration, so older databases still have the old names and uec.import_errors fails there.
      --    The uec tables were never filled on those databases, so drop and recreate them from the base DDL.
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'uec' AND table_name = 'errors' AND column_name = 'errorid'
      ) THEN
        DROP TABLE uec.errors, uec.error_definitions, uec.error_code,
                   uec.device_instance, uec.subsystem, uec.device_type;

        -- Creating uec.device_type
        CREATE TABLE IF NOT EXISTS uec.device_type (
          device_type_id INT PRIMARY KEY,
          name TEXT NOT NULL UNIQUE
        );

        -- Creating uec.device_instance
        CREATE TABLE IF NOT EXISTS uec.device_instance (
          device_instance_id INT NOT NULL,
          device_type_id INT NOT NULL REFERENCES uec.device_type (device_type_id),
          name TEXT NOT NULL,
          PRIMARY KEY (device_instance_id, device_type_id),
          UNIQUE (device_type_id, name)
        );

        -- Creating uec.error_code
        CREATE TABLE IF NOT EXISTS uec.error_code (
          device_type_id INT NOT NULL REFERENCES uec.device_type (device_type_id),
          error_code_id INT NOT NULL,
          name TEXT NOT NULL,
          PRIMARY KEY (device_type_id, error_code_id)
        );

        -- Creating uec.subsystem
        CREATE TABLE IF NOT EXISTS uec.subsystem (
          subsystem_id INT PRIMARY KEY,
          name TEXT NOT NULL UNIQUE
        );

        -- Creating uec.error_definitions
        CREATE TABLE IF NOT EXISTS uec.error_definitions (
          error_definition_id INT PRIMARY KEY,
          subsystem_id INT NOT NULL REFERENCES uec.subsystem (subsystem_id),
          device_type_id INT NOT NULL REFERENCES uec.device_type (device_type_id),
          error_code_id INT NOT NULL,
          device_instance_id INT NOT NULL,
          UNIQUE (error_code_id, subsystem_id, device_type_id, device_instance_id),
          FOREIGN KEY (device_instance_id, device_type_id) REFERENCES uec.device_instance (device_instance_id, device_type_id),
          FOREIGN KEY (device_type_id, error_code_id) REFERENCES uec.error_code (device_type_id, error_code_id)
        );

        -- Creating uec.errors
        CREATE TABLE IF NOT EXISTS uec.errors (
          time timestamptz NOT NULL,
          instrument_id BIGINT NOT NULL REFERENCES events.instruments (id) ON DELETE CASCADE,
          error_id INT NOT NULL REFERENCES uec.error_definitions (error_definition_id) ON DELETE CASCADE,
          message TEXT,
          UNIQUE (instrument_id, error_id, time)
        );

        COMMENT ON TABLE uec.errors IS 'Main UEC table with alarm events';
      END IF;

      -- 2. uec.import_errors, first run fixes:
      --    a) no errors were imported for an instrument, because MAX(time) of its empty uec.errors is NULL
      --       and "ErrorDtm > NULL" is never true
      --    b) error definition 0 (the catalogue starts at 0) was skipped, so its errors were dropped too
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
    -- -1 on the first run: the UEC catalogue starts at ErrorDefinitionID 0
    WHERE edf.ErrorDefinitionID > COALESCE((SELECT MAX(error_definition_id) FROM uec.error_definitions), -1)
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
    -- '-infinity' on the first run, when the instrument has no errors yet (MAX is NULL)
    WHERE en.ErrorDtm > COALESCE((SELECT MAX(time) FROM uec.errors WHERE instrument_id = $1), '-infinity')
    ON CONFLICT (time, instrument_id, error_id) DO NOTHING
  $q$, v_schema) USING v_instrument_id;
END;
$func$;
$sql$;

      REVOKE ALL ON PROCEDURE uec.import_errors(INT, JSONB) FROM PUBLIC;
      SET ROLE postgres;

      -- 3. Update schema version
      UPDATE public.schema_info SET version = 13, updated = NOW();
    END IF;
  END
$$
