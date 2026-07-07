DO $$
DECLARE
    current_version INTEGER;
BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 4 THEN
        -- 1. Change pganalyze schema&tables owner
        ALTER SCHEMA pganalyze OWNER TO pganalyze;
        ALTER ROLE pganalyze SET search_path = pganalyze,public;
        ALTER TABLE pganalyze.database_stats OWNER TO pganalyze;
        ALTER TABLE pganalyze.table_stats OWNER TO pganalyze;
        ALTER TABLE pganalyze.index_stats OWNER TO pganalyze;
        ALTER TABLE pganalyze.vacuum_stats OWNER TO pganalyze;
        ALTER TABLE pganalyze.stat_snapshots OWNER TO pganalyze;
        ALTER TABLE pganalyze.queries OWNER TO pganalyze;
        ALTER TABLE pganalyze.stat_statements OWNER TO pganalyze;
        ALTER TABLE pganalyze.stat_explains OWNER TO pganalyze;
        ALTER TABLE pganalyze.sys_stats OWNER TO pganalyze;

        -- 8. Update schema version
        UPDATE public.schema_info SET version = 5;
    END IF;
END $$
