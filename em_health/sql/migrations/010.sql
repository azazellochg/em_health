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

      -- 3. Enum members: the value is the identity (events.data stores it), HM can rename members in place
      ALTER TABLE events.enum_values DROP CONSTRAINT IF EXISTS enum_values_names_key;
      ALTER TABLE events.enum_values DROP CONSTRAINT IF EXISTS enum_values_values_key;
      ALTER TABLE events.enum_values
        ADD CONSTRAINT enum_values_instr_enum_value_pkey PRIMARY KEY (instrument_id, enum_id, value);
      COMMENT ON CONSTRAINT enum_values_instr_enum_value_pkey
        ON events.enum_values IS 'The value identifies a member (events.data stores it); HM can rename members in place';

      -- 4. Import: rename enum members by value instead of failing on the old unique names
      EXECUTE $sql$
CREATE OR REPLACE FUNCTION events.import_instrument(
  p_instr_dict JSONB,
  p_config_hash BYTEA,
  p_config_dict JSONB,
  OUT v_instrument_id BIGINT
)
  RETURNS BIGINT
  LANGUAGE plpgsql
AS $func$
DECLARE
  v_current_config_hash BYTEA;
  v_config_id BIGINT;
  v_renamed RECORD;
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

  -- Report enum members renamed in place: the value is the member identity
  -- (it is what events.data stores), the name is only a label that HM can change
  FOR v_renamed IN
    SELECT
      et.name AS enum_name,
      existing.value,
      existing.member_name AS old_name,
      ev.key AS new_name
    FROM jsonb_each(p_config_dict->'enums') AS e(key, value)
         JOIN events.enum_types AS et
        ON et.instrument_id = v_instrument_id
        AND et.name = e.key
         CROSS JOIN LATERAL jsonb_each_text(e.value) AS ev(key, value)
         JOIN events.enum_values AS existing
        ON existing.instrument_id = v_instrument_id
        AND existing.enum_id = et.enum_id
        AND existing.value = ev.value::INTEGER
    WHERE existing.member_name <> ev.key
  LOOP
    RAISE NOTICE '[%] Enum % value % renamed: % -> %',
      p_instr_dict ->> 'name',
      v_renamed.enum_name,
      v_renamed.value,
      v_renamed.old_name,
      v_renamed.new_name;
  END LOOP;

  -- Insert new enum values, rename existing ones
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
  ON CONFLICT (instrument_id, enum_id, value)
    DO UPDATE SET
    member_name = EXCLUDED.member_name
  WHERE events.enum_values.member_name IS DISTINCT FROM EXCLUDED.member_name;

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
            CASE
                WHEN (p.data ->> 'abs_min')::numeric = '-1.7976931348623157e308'::numeric
                    THEN NULL
                ELSE (p.data ->> 'abs_min')::numeric
                END,
            CASE
                WHEN (p.data ->> 'abs_max')::numeric = '1.7976931348623157e308'::numeric
                    THEN NULL
                ELSE (p.data ->> 'abs_max')::numeric
                END,
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
$func$;
$sql$;

      -- 5. Warn when parameter units change
      EXECUTE $sql$
CREATE OR REPLACE FUNCTION events.parameters_log_after_update() RETURNS TRIGGER AS
$func$
BEGIN
  IF ROW(
    old.instrument_id,
    old.param_id,
    old.config_id,
    old.enum_id,
    old.event_id,
    old.abs_limits,
    old.crit_limits,
    old.warn_limits,
    old.caution_limits,
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
    new.crit_limits,
    new.warn_limits,
    new.caution_limits,
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
      crit_limits,
      warn_limits,
      caution_limits,
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
        old.crit_limits,
        old.warn_limits,
        old.caution_limits,
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

    -- Same param_id, different units: events.data values before and after this change are not comparable
    IF ROW(old.storage_unit, old.display_unit) IS DISTINCT FROM ROW(new.storage_unit, new.display_unit) THEN
      RAISE WARNING 'Units changed for parameter % (%) of instrument %: storage % -> %, display % -> %',
        old.param_id, old.param_name, old.instrument_id,
        old.storage_unit, new.storage_unit, old.display_unit, new.display_unit;
    END IF;
  END IF;

  RETURN new;
END;
$func$ LANGUAGE plpgsql;
$sql$;

      -- 6. Chunk skipping on param_id never excludes a chunk: param ids are per instrument
      -- and every chunk holds almost all params, so each chunk range is the full id range
      PERFORM disable_chunk_skipping('events.data', 'param_id', if_not_exists => TRUE);

      -- 7. Drop the "_new" suffix left by migration 007 (constraints, their indexes, sequences)
      FOR r IN
        SELECT c.conrelid::regclass AS tbl,
               c.conname,
               regexp_replace(c.conname, '^' || t.relname || '_new_', t.relname || '_') AS new_name
        FROM pg_constraint c
             JOIN pg_class t ON t.oid = c.conrelid
        WHERE c.connamespace = 'events'::regnamespace
          AND c.conname LIKE t.relname || '\_new\_%'
      LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_constraint
                       WHERE conrelid = r.tbl AND conname = r.new_name) THEN
          EXECUTE format('ALTER TABLE %s RENAME CONSTRAINT %I TO %I', r.tbl, r.conname, r.new_name);
        END IF;
      END LOOP;

      FOR r IN
        SELECT s.relname,
               regexp_replace(s.relname, '^(instruments|enum_types)_new_', '\1_') AS new_name
        FROM pg_class s
        WHERE s.relnamespace = 'events'::regnamespace
          AND s.relkind = 'S'
          AND s.relname ~ '^(instruments|enum_types)_new_'
      LOOP
        IF TO_REGCLASS(format('events.%I', r.new_name)) IS NULL THEN
          EXECUTE format('ALTER SEQUENCE events.%I RENAME TO %I', r.relname, r.new_name);
        END IF;
      END LOOP;

      -- 8. Update schema version
      UPDATE public.schema_info SET version = 10, updated = NOW();
    END IF;
  END
$$
