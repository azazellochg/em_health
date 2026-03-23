DO $$
DECLARE
    current_version INTEGER;
BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 3 THEN
        -- 1. Remove unused column from index_stats
        ALTER TABLE pganalyze.index_stats DROP COLUMN exclusively_locked;

        -- 2. Update schema version
        UPDATE public.schema_info SET version = 4;
    END IF;
END $$
