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
            ALTER TABLE pganalyze.stat_statements ADD COLUMN userid OID;

            UPDATE pganalyze.stat_statements
            SET userid = grafana_oid;

            ALTER TABLE pganalyze.stat_statements
            ALTER COLUMN userid SET NOT NULL;
        END IF;

        -- 2. Replace primary key
        ALTER TABLE pganalyze.stat_statements
        DROP CONSTRAINT stat_statements_pkey;

        ALTER TABLE pganalyze.stat_statements
        ADD PRIMARY KEY (queryid, userid, collected_at);

        -- 3. Replace wal_bytes column with BIGINT
        SELECT data_type
        INTO col_type
        FROM information_schema.columns
        WHERE table_schema = 'pganalyze'
          AND table_name = 'stat_statements'
          AND column_name = 'wal_bytes';

        IF col_type IS NOT NULL AND col_type <> 'bigint' THEN
            ALTER TABLE pganalyze.stat_statements DROP COLUMN wal_bytes;
            ALTER TABLE pganalyze.stat_statements
            ADD COLUMN wal_bytes BIGINT NOT NULL DEFAULT 0;
        END IF;

        -- 4. Create emhealth user
        SELECT EXISTS (
            SELECT 1 FROM pg_roles WHERE rolname = 'emhealth'
        ) INTO role_exists;

        IF NOT role_exists THEN
            CREATE ROLE emhealth WITH LOGIN PASSWORD 'emhealth';
        END IF;

        GRANT USAGE ON SCHEMA public TO emhealth;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO emhealth;
        GRANT TRUNCATE ON TABLE public.data_staging TO emhealth;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO emhealth;
        GRANT USAGE ON SCHEMA uec TO emhealth;
        GRANT SELECT, DELETE ON ALL TABLES IN SCHEMA uec TO emhealth;
        ALTER DEFAULT PRIVILEGES IN SCHEMA uec GRANT SELECT, DELETE ON TABLES TO emhealth;

        -- 5. Drop unused indices
        DROP INDEX IF EXISTS enum_types_instrument_id_name_idx;
        DROP INDEX IF EXISTS parameters_instrument_id_param_id_idx;
        DROP INDEX IF EXISTS pganalyze.stat_statements_queryid_time;
        DROP INDEX IF EXISTS uec.idx_errors_instrument_time;
        DROP INDEX IF EXISTS data_instrument_id_param_id_time_idx;

        PERFORM disable_chunk_skipping('public.data', 'instrument_id');

        -- 6. Replace unique constraint on data. param_id is equality constraint with highest cardinality
        ALTER TABLE data DROP CONSTRAINT data_time_instrument_id_param_id_key;
        ALTER TABLE data ADD UNIQUE (param_id, instrument_id, time);

        -- 7. Update schema version
        UPDATE public.schema_info SET version = 2;
    END IF;
END $$;
