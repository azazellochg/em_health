-- Creating purge_old_chunks
CREATE OR REPLACE FUNCTION events.purge_old_chunks(
  p_hypertable_name TEXT,
  p_retain_days INT,
  OUT chunks_dropped INT
)
  LANGUAGE plpgsql AS
$$
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
$$;

-- Creating import_instrument
CREATE OR REPLACE FUNCTION events.import_instrument(
  p_instr_dict JSONB,
  p_config_hash BYTEA,
  p_config_dict JSONB,
  OUT v_instrument_id BIGINT
)
  RETURNS BIGINT
  LANGUAGE plpgsql
AS $$
DECLARE
  v_current_config_hash BYTEA;
  v_config_id BIGINT;
  v_mismatch RECORD;
BEGIN
  -- Find the instrument and lock it so concurrent imports for the same instrument cannot race
  SELECT i.id, c.config_hash
  INTO v_instrument_id, v_current_config_hash
  FROM events.instruments AS i
       LEFT JOIN events.configurations AS c
      ON c.id = i.current_config_id
  WHERE i.instrument = p_instr_dict ->> 'instrument'
    FOR UPDATE OF i;

  -- Instrument does not exist
  IF NOT FOUND THEN
    RAISE NOTICE '[%] Instrument not found, creating a new entry', p_instr_dict ->> 'name';

    INSERT INTO events.instruments (
      serial,
      server,
      instrument,
      model,
      name,
      template
    )
    VALUES (
      (p_instr_dict ->> 'serial')::INTEGER,
      (p_instr_dict ->> 'server')::INET,
      p_instr_dict ->> 'instrument',
      p_instr_dict ->> 'model',
      p_instr_dict ->> 'name',
      p_instr_dict ->> 'template'
    )
    RETURNING id INTO v_instrument_id;

    INSERT INTO events.configurations (
      instrument_id,
      config_hash,
      config_dict
    )
    VALUES (
      v_instrument_id,
      p_config_hash,
      p_config_dict
    )
    RETURNING id INTO v_config_id;

    UPDATE events.instruments
    SET current_config_id = v_config_id
    WHERE id = v_instrument_id;

  ELSE
    RAISE NOTICE '[%] Instrument already exists', p_instr_dict ->> 'name';

    IF v_current_config_hash IS DISTINCT FROM p_config_hash THEN
      -- Configuration changed: create a new config
      RAISE NOTICE '[%] Configuration has changed, updating', p_instr_dict ->> 'name';
      INSERT INTO events.configurations (
        instrument_id,
        config_hash,
        config_dict
      )
      VALUES (
        v_instrument_id,
        p_config_hash,
        p_config_dict
      )
      RETURNING id INTO v_config_id;

      -- Make the new configuration current
      UPDATE events.instruments
      SET current_config_id = v_config_id
      WHERE id = v_instrument_id;
    END IF;

  END IF;

  -- Insert enum types, on conflict explicitly set is_active=TRUE
  INSERT INTO events.enum_types (
    instrument_id,
    name
  )
  SELECT
    v_instrument_id,
    key
  FROM jsonb_each(p_config_dict->'enums')
  ON CONFLICT (instrument_id, name)
    DO UPDATE SET
    is_active = TRUE
  WHERE events.enum_types.is_active IS DISTINCT FROM TRUE;

  -- Deactivate enum types no longer present in the config
  UPDATE events.enum_types et
  SET is_active = FALSE
  WHERE et.instrument_id = v_instrument_id
    AND et.is_active = TRUE
    AND NOT EXISTS (
      SELECT 1
      FROM jsonb_object_keys(p_config_dict->'enums') AS e(name)
      WHERE e.name = et.name
    );

  -- Check for mismatch in enum values for existing members
  FOR v_mismatch IN
    SELECT
      et.instrument_id,
      et.enum_id,
      et.name AS enum_name,
      ev.key AS member_name,
      existing.value AS old_value,
      ev.value::INTEGER AS new_value
    FROM jsonb_each(p_config_dict->'enums') AS e(key, value)
         JOIN events.enum_types AS et
        ON et.instrument_id = v_instrument_id
        AND et.name = e.key
         CROSS JOIN LATERAL jsonb_each_text(e.value) AS ev(key, value)
         JOIN events.enum_values AS existing
        ON existing.instrument_id = v_instrument_id
        AND existing.enum_id = et.enum_id
        AND existing.member_name = ev.key
    WHERE existing.value <> ev.value::INTEGER
  LOOP
    RAISE EXCEPTION
      'Enum value mismatch: instr_id=%, enum_id=%, name=%, member=%, old_value=%, new_value=%',
      v_mismatch.instrument_id,
      v_mismatch.enum_id,
      v_mismatch.enum_name,
      v_mismatch.member_name,
      v_mismatch.old_value,
      v_mismatch.new_value;
  END LOOP;

  -- Insert enum values
  INSERT INTO events.enum_values (
    instrument_id,
    enum_id,
    value,
    member_name
  )
  SELECT
    v_instrument_id,
    et.enum_id,
    ev.value::INTEGER,
    ev.key
  FROM jsonb_each(p_config_dict->'enums') AS e(key, value)
       JOIN events.enum_types AS et
      ON et.instrument_id = v_instrument_id
      AND et.name = e.key
       CROSS JOIN LATERAL jsonb_each_text(e.value) AS ev(key, value)
  ON CONFLICT (instrument_id, enum_id, member_name)
    DO NOTHING;

  -- Insert parameters
  INSERT INTO events.parameters (
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
  SELECT
    i.id,
    (p.param_id)::BIGINT,
    i.current_config_id,
    et.enum_id,
    (p.data ->> 'event_id')::INTEGER,
    numrange(
      (p.data ->> 'abs_min')::numeric,
      (p.data ->> 'abs_max')::numeric,
      '[]'
    ),
    p.data ->> 'subsystem',
    p.data ->> 'component',
    p.data ->> 'param_name',
    p.data ->> 'display_name',
    p.data ->> 'display_unit',
    p.data ->> 'storage_unit',
    p.data ->> 'value_type',
    p.data ->> 'event_name'
  FROM events.instruments AS i
       CROSS JOIN jsonb_each(p_config_dict->'params') AS p(param_id, data)
       LEFT JOIN events.enum_types AS et
      ON et.instrument_id = v_instrument_id
      AND et.name = p.data ->> 'enum_name'
  WHERE i.id = v_instrument_id

  ON CONFLICT (instrument_id, param_id)
    DO UPDATE SET
    config_id = EXCLUDED.config_id,
    enum_id = EXCLUDED.enum_id,
    event_id = EXCLUDED.event_id,
    abs_limits = EXCLUDED.abs_limits,
    subsystem = EXCLUDED.subsystem,
    component = EXCLUDED.component,
    param_name = EXCLUDED.param_name,
    display_name = EXCLUDED.display_name,
    display_unit = EXCLUDED.display_unit,
    storage_unit = EXCLUDED.storage_unit,
    value_type = EXCLUDED.value_type,
    event_name = EXCLUDED.event_name,
    is_active = TRUE

  WHERE ROW(
    events.parameters.enum_id,
    events.parameters.event_id,
    events.parameters.abs_limits,
    events.parameters.subsystem,
    events.parameters.component,
    events.parameters.param_name,
    events.parameters.display_name,
    events.parameters.display_unit,
    events.parameters.storage_unit,
    events.parameters.value_type,
    events.parameters.event_name,
    events.parameters.is_active
    ) IS DISTINCT FROM ROW(
    EXCLUDED.enum_id,
    EXCLUDED.event_id,
    EXCLUDED.abs_limits,
    EXCLUDED.subsystem,
    EXCLUDED.component,
    EXCLUDED.param_name,
    EXCLUDED.display_name,
    EXCLUDED.display_unit,
    EXCLUDED.storage_unit,
    EXCLUDED.value_type,
    EXCLUDED.event_name,
    TRUE
    );

  -- Deactivate params no longer present in the config
  UPDATE events.parameters p
  SET is_active = FALSE
  WHERE p.instrument_id = v_instrument_id
    AND p.is_active = TRUE
    AND NOT EXISTS (
      SELECT 1
      FROM jsonb_object_keys(p_config_dict->'params') AS param(param_id)
      WHERE param.param_id::BIGINT = p.param_id
    );

  RETURN;
END;
$$;

-- Creating delete_instrument
CREATE OR REPLACE FUNCTION events.delete_instrument(
  p_instrument_id BIGINT
)
  RETURNS VOID
  LANGUAGE plpgsql
AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM events.instruments
        WHERE id = p_instrument_id
    ) THEN
        RAISE EXCEPTION 'Instrument % does not exist', p_instrument_id;
    END IF;

    DELETE FROM events.data
    WHERE instrument_id = p_instrument_id;

    DELETE FROM events.parameters_history
    WHERE instrument_id = p_instrument_id;

    DELETE FROM events.enum_values
    WHERE instrument_id = p_instrument_id;

    DELETE FROM events.parameters
    WHERE instrument_id = p_instrument_id;

    DELETE FROM events.enum_types
    WHERE instrument_id = p_instrument_id;

    DELETE FROM events.configurations
    WHERE instrument_id = p_instrument_id;

    DELETE FROM events.instruments
    WHERE id = p_instrument_id;
END;
$$;
