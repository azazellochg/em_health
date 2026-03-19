DO $$
DECLARE
    current_version INTEGER;
BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 3 THEN
        -- 1. Remove deprecated mview

        -- 2. Remove unused column from index_stats
        ALTER TABLE pganalyze.stat_statements DROP COLUMN exclusively_locked;

        -- 3. Update schema version
        UPDATE public.schema_info SET version = 4;
    END IF;
END $$
