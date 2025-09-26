DO $$
DECLARE
    current_version INTEGER;
BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 2 THEN
        -- tbd

        -- N. Update schema version
        UPDATE public.schema_info SET version = 3;
    END IF;
END $$;
