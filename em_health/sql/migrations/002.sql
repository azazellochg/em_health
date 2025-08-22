DO $$
    DECLARE
        current_version INTEGER;
    BEGIN
        SELECT MAX(version) INTO current_version FROM public.schema_info;

        IF current_version = 1 THEN
            -- tbd

            UPDATE public.schema_info SET version = 2;
        END IF;
    END $$;
