DO $$
DECLARE
    current_version INTEGER;
    grafana_oid OID;
    col_exists BOOLEAN;
    col_type TEXT;
    role_exists BOOLEAN;
BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 1 THEN
        -- Resolve grafana role OID once
        SELECT 'grafana'::regrole::oid INTO grafana_oid;

        -- 1. Add userid column if it doesn't already exist
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'pganalyze'
              AND table_name = 'stat_statements'
              AND column_name = 'userid'
        ) INTO col_exists;

        IF NOT col_exists THEN
            EXECUTE 'ALTER TABLE pganalyze.stat_statements ADD COLUMN userid OID';

            EXECUTE format(
                'UPDATE pganalyze.stat_statements SET userid = %s',
                grafana_oid
            );

            EXECUTE 'ALTER TABLE pganalyze.stat_statements ALTER COLUMN userid SET NOT NULL';
        END IF;

        -- 2. Replace primary key
        EXECUTE 'ALTER TABLE pganalyze.stat_statements DROP CONSTRAINT stat_statements_pkey';
        EXECUTE 'ALTER TABLE pganalyze.stat_statements ADD PRIMARY KEY (collected_at, userid, queryid)';

        -- 3. Replace wal_bytes column with BIGINT
        SELECT data_type
        INTO col_type
        FROM information_schema.columns
        WHERE table_schema = 'pganalyze'
          AND table_name = 'stat_statements'
          AND column_name = 'wal_bytes';

        IF col_type IS NOT NULL AND col_type <> 'bigint' THEN
            EXECUTE 'ALTER TABLE pganalyze.stat_statements DROP COLUMN wal_bytes';
            EXECUTE 'ALTER TABLE pganalyze.stat_statements ADD COLUMN wal_bytes BIGINT NOT NULL DEFAULT 0';
        END IF;

        -- 4. Create emhealth user
        SELECT EXISTS (
            SELECT 1 FROM pg_roles WHERE rolname = 'emhealth'
        ) INTO role_exists;

        IF NOT role_exists THEN
            EXECUTE 'CREATE ROLE emhealth WITH LOGIN PASSWORD ''emhealth''';
        END IF;

        EXECUTE 'GRANT USAGE ON SCHEMA public TO emhealth';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO emhealth';
        EXECUTE 'GRANT TRUNCATE ON TABLE public.data_staging TO emhealth';
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO emhealth';
        EXECUTE 'GRANT USAGE ON SCHEMA uec TO emhealth';
        EXECUTE 'GRANT SELECT, DELETE ON ALL TABLES IN SCHEMA uec TO emhealth';
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA uec GRANT SELECT, DELETE ON TABLES TO emhealth';

        -- 6. Update schema version
        UPDATE public.schema_info SET version = 2;
    END IF;
END $$;
