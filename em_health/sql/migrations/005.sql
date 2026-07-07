DO $$
DECLARE
    current_version INTEGER;
BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 4 THEN
        -- 1. Change pganalyze schema&tables owner
        COMMENT ON SCHEMA pganalyze IS 'DB performance statistics';
        ALTER SCHEMA pganalyze OWNER TO pganalyze;
        ALTER TABLE pganalyze.database_stats OWNER TO pganalyze;
        ALTER TABLE pganalyze.table_stats OWNER TO pganalyze;
        ALTER TABLE pganalyze.index_stats OWNER TO pganalyze;
        ALTER TABLE pganalyze.vacuum_stats OWNER TO pganalyze;
        ALTER TABLE pganalyze.stat_snapshots OWNER TO pganalyze;
        ALTER TABLE pganalyze.queries OWNER TO pganalyze;
        ALTER TABLE pganalyze.stat_statements OWNER TO pganalyze;
        ALTER TABLE pganalyze.stat_explains OWNER TO pganalyze;
        ALTER TABLE pganalyze.sys_stats OWNER TO pganalyze;

        -- 2. Change uec schema&tables owner
        COMMENT ON SCHEMA uec IS 'Error codes';
        ALTER SCHEMA uec OWNER TO emhealth;
        ALTER TABLE uec.device_type OWNER TO emhealth;
        ALTER TABLE uec.device_instance OWNER TO emhealth;
        ALTER TABLE uec.error_code OWNER TO emhealth;
        ALTER TABLE uec.subsystem OWNER TO emhealth;
        ALTER TABLE uec.error_definitions OWNER TO emhealth;
        ALTER TABLE uec.errors OWNER TO emhealth;

        -- 3. Move public tables to the new events schema
        CREATE SCHEMA IF NOT EXISTS events AUTHORIZATION emhealth;
        COMMENT ON SCHEMA events IS 'HM events';

        GRANT USAGE ON SCHEMA events TO grafana;
        GRANT SELECT ON ALL TABLES IN SCHEMA events TO grafana;
        ALTER DEFAULT PRIVILEGES IN SCHEMA events GRANT SELECT ON TABLES TO grafana;

        -- move tables
        ALTER TABLE public.instruments SET SCHEMA events;
        ALTER TABLE public.enum_types SET SCHEMA events;
        ALTER TABLE public.enum_values SET SCHEMA events;
        ALTER TABLE public.enum_values_history SET SCHEMA events;
        ALTER TABLE public.parameters SET SCHEMA events;
        ALTER TABLE public.parameters_history SET SCHEMA events;
        ALTER TABLE public.data_staging SET SCHEMA events;
        ALTER TABLE public.data SET SCHEMA events;

        -- update FKs
        ALTER TABLE events.enum_types DROP CONSTRAINT enum_types_instrument_id_fkey;
        ALTER TABLE events.enum_types ADD CONSTRAINT enum_types_instrument_id_fkey FOREIGN KEY (instrument_id) REFERENCES events.instruments(id) ON DELETE CASCADE;

        ALTER TABLE events.parameters DROP CONSTRAINT parameters_instrument_id_fkey;
        ALTER TABLE events.parameters ADD CONSTRAINT parameters_instrument_id_fkey FOREIGN KEY (instrument_id) REFERENCES events.instruments(id) ON DELETE CASCADE;

        ALTER TABLE events.parameters_history DROP CONSTRAINT parameters_history_instrument_id_fkey;
        ALTER TABLE events.parameters_history ADD CONSTRAINT parameters_history_instrument_id_fkey FOREIGN KEY (instrument_id) REFERENCES events.instruments(id) ON DELETE CASCADE;

        ALTER TABLE uec.errors DROP CONSTRAINT errors_instrument_id_fkey;
        ALTER TABLE uec.errors ADD CONSTRAINT errors_instrument_id_fkey FOREIGN KEY (instrument_id) REFERENCES events.instruments(id) ON DELETE CASCADE;

        -- change owner
        ALTER SCHEMA events OWNER TO emhealth;
        ALTER TABLE events.instruments OWNER TO emhealth;
        ALTER TABLE events.enum_types OWNER TO emhealth;
        ALTER TABLE events.enum_values OWNER TO emhealth;
        ALTER TABLE events.enum_values_history OWNER TO emhealth;
        ALTER TABLE events.parameters OWNER TO emhealth;
        ALTER TABLE events.parameters_history OWNER TO emhealth;
        ALTER TABLE events.data_staging OWNER TO emhealth;
        ALTER TABLE events.data OWNER TO emhealth;

        -- allow usage for grafana
        GRANT USAGE ON SCHEMA events TO grafana;
        GRANT SELECT ON ALL TABLES IN SCHEMA events TO grafana;
        ALTER DEFAULT PRIVILEGES IN SCHEMA events GRANT SELECT ON TABLES TO grafana;
        REVOKE USAGE ON SCHEMA public FROM grafana, emhealth;

        -- 4. Update search path
        ALTER ROLE emhealth SET search_path = events,uec,public;
        ALTER ROLE pganalyze SET search_path = pganalyze,public;
        ALTER ROLE grafana SET search_path = events,uec,pganalyze,public;

        -- 5. Update schema version
        COMMENT ON TABLE public.schema_info IS 'Global schema version';
        UPDATE public.schema_info SET version = 5;
    END IF;
END $$
