DO $$
    DECLARE
        current_version INTEGER;
    BEGIN
        SELECT MAX(version) INTO current_version FROM public.schema_info;

        IF current_version = 1 THEN

            -- Add column
            EXECUTE 'ALTER TABLE pganalyze.stat_statements
                 ADD COLUMN userid OID NOT NULL DEFAULT ''grafana''::regrole::oid';

            -- Drop existing PK
            EXECUTE 'ALTER TABLE pganalyze.stat_statements
                 DROP CONSTRAINT stat_statements_pkey';

            -- Add new PK
            EXECUTE 'ALTER TABLE pganalyze.stat_statements
                 ADD PRIMARY KEY (collected_at, userid, queryid)';

            -- Update schema version
            UPDATE public.schema_info SET version = 2;

        END IF;
    END $$;
