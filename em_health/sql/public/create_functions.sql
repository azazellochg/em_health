-- BEFORE INSERT triggers: upsert logic

-- Creating trigger enum_values_upsert_before_insert()
CREATE OR REPLACE FUNCTION enum_values_upsert_before_insert()
    RETURNS trigger AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.enum_values
        WHERE enum_id = NEW.enum_id
          AND member_name = NEW.member_name
    ) THEN
        UPDATE public.enum_values
        SET value = NEW.value
        WHERE enum_id = NEW.enum_id
          AND member_name = NEW.member_name;
        RETURN NULL; -- skip the insert
    ELSE
        RETURN NEW; -- proceed with insert
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER enum_values_upsert_before_insert
BEFORE INSERT ON public.enum_values
FOR EACH ROW
EXECUTE FUNCTION enum_values_upsert_before_insert();


-- Creating trigger parameters_upsert_before_insert()
CREATE OR REPLACE FUNCTION parameters_upsert_before_insert()
RETURNS trigger AS $$
BEGIN
    -- If exists, update instead of insert
    IF EXISTS (
        SELECT 1
        FROM public.parameters
        WHERE instrument_id = NEW.instrument_id
          AND param_id = NEW.param_id
    ) THEN
        UPDATE public.parameters
        SET subsystem = NEW.subsystem,
            component = NEW.component,
            param_name = NEW.param_name,
            display_name = NEW.display_name,
            display_unit = NEW.display_unit,
            storage_unit = NEW.storage_unit,
            enum_id = NEW.enum_id,
            value_type = NEW.value_type,
            event_id = NEW.event_id,
            event_name = NEW.event_name,
            abs_min = NEW.abs_min,
            abs_max = NEW.abs_max
        WHERE instrument_id = NEW.instrument_id
          AND param_id = NEW.param_id;
        RETURN NULL; -- skip insert
    ELSE
        RETURN NEW; -- proceed with insert
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER parameters_upsert_before_insert
BEFORE INSERT ON public.parameters
FOR EACH ROW
EXECUTE FUNCTION parameters_upsert_before_insert();


-- AFTER UPDATE triggers: log old values to history

-- Creating trigger enum_values_log_after_update()
CREATE OR REPLACE FUNCTION enum_values_log_after_update()
    RETURNS trigger AS $$
BEGIN
    IF ROW(OLD.*) IS DISTINCT FROM ROW(NEW.*) THEN
        INSERT INTO public.enum_values_history (enum_id, member_name, value)
        VALUES (OLD.enum_id,OLD.member_name, OLD.value);
        RAISE NOTICE 'Updated enum_values for enum_id %', OLD.enum_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER enum_values_log_after_update
AFTER UPDATE ON public.enum_values
FOR EACH ROW
EXECUTE FUNCTION enum_values_log_after_update();


-- Creating trigger parameters_log_after_update()
CREATE OR REPLACE FUNCTION parameters_log_after_update()
RETURNS trigger AS $$
BEGIN
    IF ROW(OLD.*) IS DISTINCT FROM ROW(NEW.*) THEN
        INSERT INTO public.parameters_history (
            instrument_id, param_id, subsystem, component, param_name, display_name,
            display_unit, storage_unit, enum_id, value_type, event_id, event_name,
            abs_min, abs_max
        )
        VALUES (
            OLD.instrument_id, OLD.param_id, OLD.subsystem, OLD.component, OLD.param_name, OLD.display_name,
            OLD.display_unit, OLD.storage_unit, OLD.enum_id, OLD.value_type, OLD.event_id, OLD.event_name,
            OLD.abs_min, OLD.abs_max
        );

        RAISE NOTICE 'Updated parameter % (instrument %)', NEW.param_id, NEW.instrument_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER parameters_log_after_update
AFTER UPDATE ON public.parameters
FOR EACH ROW
EXECUTE FUNCTION parameters_log_after_update();
