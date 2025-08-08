DO $$
    DECLARE
        current_version INTEGER;
    BEGIN
        SELECT version INTO current_version FROM public.schema_info LIMIT 1;

        IF current_version = 1 THEN
            CREATE SCHEMA IF NOT EXISTS uec;
            CREATE TABLE IF NOT EXISTS uec.error_definitions (
                                                                 error_definition_id INTEGER NOT NULL,
                                                                 subsystem_id INTEGER NOT NULL,
                                                                 subsystem TEXT NOT NULL,
                                                                 device_type_id INTEGER NOT NULL,
                                                                 device_type TEXT NOT NULL,
                                                                 device_instance_id INTEGER NOT NULL,
                                                                 device_instance TEXT NOT NULL,
                                                                 error_code_id INTEGER NOT NULL,
                                                                 error_code TEXT NOT NULL
            );
            CREATE UNIQUE INDEX idx_error_def_id ON uec.error_definitions (error_definition_id);

            CREATE TABLE IF NOT EXISTS uec.errors (
                                                    time TIMESTAMPTZ NOT NULL,
                                                    instrument_id INTEGER NOT NULL REFERENCES public.instruments(id),
                                                    error_id INTEGER NOT NULL REFERENCES uec.error_definitions(error_definition_id),
                                                    message_text TEXT,
                                                    UNIQUE (time, instrument_id, error_id)
            );
            CREATE INDEX ON uec.errors (instrument_id, time ASC);

            GRANT USAGE ON SCHEMA uec TO grafana;
            GRANT SELECT ON ALL TABLES IN SCHEMA uec TO grafana;

            UPDATE public.schema_info SET version = 2;
        END IF;
    END $$;
