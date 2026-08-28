-- BEFORE INSERT triggers: upsert logic

-- Creating trigger enum_values_upsert_before_insert()
CREATE OR REPLACE FUNCTION events.enum_values_upsert_before_insert() RETURNS TRIGGER AS
$$
BEGIN
  IF EXISTS (
    SELECT 1 FROM events.enum_values WHERE enum_id = new.enum_id AND member_name = new.member_name
  ) THEN
    UPDATE events.enum_values SET value = new.value WHERE enum_id = new.enum_id AND member_name = new.member_name;
    RETURN NULL; -- skip the insert
  ELSE
    RETURN new; -- proceed with insert
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER enum_values_upsert_before_insert
  BEFORE INSERT
  ON events.enum_values
  FOR EACH ROW
EXECUTE FUNCTION events.enum_values_upsert_before_insert();


-- Creating trigger parameters_upsert_before_insert()
CREATE OR REPLACE FUNCTION events.parameters_upsert_before_insert() RETURNS TRIGGER AS
$$
BEGIN
  -- If exists, update instead of insert
  IF EXISTS (
    SELECT 1 FROM events.parameters WHERE instrument_id = new.instrument_id AND param_id = new.param_id
  ) THEN
    UPDATE events.parameters
    SET
      subsystem = new.subsystem,
      component = new.component,
      param_name = new.param_name,
      display_name = new.display_name,
      display_unit = new.display_unit,
      storage_unit = new.storage_unit,
      enum_id = new.enum_id,
      value_type = new.value_type,
      event_id = new.event_id,
      event_name = new.event_name,
      abs_min = new.abs_min,
      abs_max = new.abs_max
    WHERE
      instrument_id = new.instrument_id
      AND param_id = new.param_id;
    RETURN NULL; -- skip insert
  ELSE
    RETURN new; -- proceed with insert
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER parameters_upsert_before_insert
  BEFORE INSERT
  ON events.parameters
  FOR EACH ROW
EXECUTE FUNCTION events.parameters_upsert_before_insert();


-- AFTER UPDATE triggers: log old values to history

-- Creating trigger enum_values_log_after_update()
CREATE OR REPLACE FUNCTION events.enum_values_log_after_update() RETURNS TRIGGER AS
$$
BEGIN
  IF ROW (old.*) IS DISTINCT FROM ROW (new.*) THEN
    INSERT INTO events.enum_values_history (
      enum_id,
      member_name,
      value
    )
    VALUES
      (
        old.enum_id,
        old.member_name,
        old.value
      );
    RAISE NOTICE 'Updated enum_values for enum_id %', old.enum_id;
  END IF;

  RETURN new;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER enum_values_log_after_update
  AFTER UPDATE
  ON events.enum_values
  FOR EACH ROW
EXECUTE FUNCTION events.enum_values_log_after_update();


-- Creating trigger parameters_log_after_update()
CREATE OR REPLACE FUNCTION events.parameters_log_after_update() RETURNS TRIGGER AS
$$
BEGIN
  IF ROW (old.*) IS DISTINCT FROM ROW (new.*) THEN
    INSERT INTO events.parameters_history (
      instrument_id,
      param_id,
      subsystem,
      component,
      param_name,
      display_name,
      display_unit,
      storage_unit,
      enum_id,
      value_type,
      event_id,
      event_name,
      abs_min,
      abs_max
    )
    VALUES
      (
        old.instrument_id,
        old.param_id,
        old.subsystem,
        old.component,
        old.param_name,
        old.display_name,
        old.display_unit,
        old.storage_unit,
        old.enum_id,
        old.value_type,
        old.event_id,
        old.event_name,
        old.abs_min,
        old.abs_max
      );

    RAISE NOTICE 'Updated parameter % (instrument %)', new.param_id, new.instrument_id;
  END IF;

  RETURN new;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER parameters_log_after_update
  AFTER UPDATE
  ON events.parameters
  FOR EACH ROW
EXECUTE FUNCTION events.parameters_log_after_update()
