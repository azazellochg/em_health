DO $$
    DECLARE
        current_version INTEGER;
    BEGIN
        SELECT version INTO current_version FROM public.schema_info LIMIT 1;

        IF current_version = 1 THEN
            -- tbd

            UPDATE public.schema_info SET version = 2;
        END IF;
    END $$;
