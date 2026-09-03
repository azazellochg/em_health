-- AFTER UPDATE triggers: log old values to history

-- Creating trigger parameters_log_after_update()
CREATE OR REPLACE FUNCTION events.parameters_log_after_update() RETURNS TRIGGER AS
$$
BEGIN
  IF ROW(
    old.instrument_id,
    old.param_id,
    old.config_id,
    old.enum_id,
    old.event_id,
    old.abs_limits,
    old.subsystem,
    old.component,
    old.param_name,
    old.display_name,
    old.display_unit,
    old.storage_unit,
    old.value_type,
    old.event_name
    ) IS DISTINCT FROM ROW(
    new.instrument_id,
    new.param_id,
    new.config_id,
    new.enum_id,
    new.event_id,
    new.abs_limits,
    new.subsystem,
    new.component,
    new.param_name,
    new.display_name,
    new.display_unit,
    new.storage_unit,
    new.value_type,
    new.event_name
    ) THEN
    INSERT INTO events.parameters_history (
      instrument_id,
      param_id,
      config_id,
      enum_id,
      event_id,
      abs_limits,
      subsystem,
      component,
      param_name,
      display_name,
      display_unit,
      storage_unit,
      value_type,
      event_name
    )
    VALUES
      (
        old.instrument_id,
        old.param_id,
        old.config_id,
        old.enum_id,
        old.event_id,
        old.abs_limits,
        old.subsystem,
        old.component,
        old.param_name,
        old.display_name,
        old.display_unit,
        old.storage_unit,
        old.value_type,
        old.event_name
      );

    RAISE NOTICE E'Updated parameter % for instrument %:\n%\n->\n%', old.param_id, old.instrument_id, old, new;
  END IF;

  RETURN new;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER parameters_log_after_update
  AFTER UPDATE
  ON events.parameters
  FOR EACH ROW
EXECUTE FUNCTION events.parameters_log_after_update()
