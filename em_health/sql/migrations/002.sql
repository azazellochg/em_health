DO $$
DECLARE
    current_version INTEGER;
    grafana_oid OID;
BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 1 THEN
        -- Resolve grafana role OID once
        SELECT 'grafana'::regrole::oid INTO grafana_oid;

        -- 1. Add new column (nullable initially)
        EXECUTE 'ALTER TABLE pganalyze.stat_statements ADD COLUMN userid OID';

        -- 2. Backfill with grafana oid
        EXECUTE format(
            'UPDATE pganalyze.stat_statements SET userid = %s',
            grafana_oid
        );

        -- 3. Enforce NOT NULL
        EXECUTE 'ALTER TABLE pganalyze.stat_statements ALTER COLUMN userid SET NOT NULL';

        -- 4. Replace primary key
        EXECUTE 'ALTER TABLE pganalyze.stat_statements DROP CONSTRAINT stat_statements_pkey';
        EXECUTE 'ALTER TABLE pganalyze.stat_statements ADD PRIMARY KEY (collected_at, userid, queryid)';

        -- 5. Replace wal_bytes column with BIGINT
        EXECUTE 'ALTER TABLE pganalyze.stat_statements DROP COLUMN wal_bytes';
        EXECUTE 'ALTER TABLE pganalyze.stat_statements ADD COLUMN wal_bytes BIGINT NOT NULL DEFAULT 0';

        -- 6. Create emhealth user
        EXECUTE 'CREATE ROLE emhealth WITH LOGIN PASSWORD ''emhealth''';
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
