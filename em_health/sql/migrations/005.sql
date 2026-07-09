DO $$
DECLARE
    current_version INTEGER;
    purge_func_exists BOOLEAN;
    dbname text := current_database();
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

        ALTER FUNCTION pganalyze.parse_logs(integer, jsonb) SECURITY INVOKER;
        ALTER FUNCTION pganalyze.parse_sysinfo(integer, jsonb) SECURITY INVOKER;
        ALTER FUNCTION pganalyze.purge_stats(integer, jsonb) SECURITY INVOKER;

        -- 3. Change uec schema&tables owner
        COMMENT ON SCHEMA uec IS 'Error codes';
        ALTER SCHEMA uec OWNER TO emhealth;
        ALTER TABLE uec.device_type OWNER TO emhealth;
        ALTER TABLE uec.device_instance OWNER TO emhealth;
        ALTER TABLE uec.error_code OWNER TO emhealth;
        ALTER TABLE uec.subsystem OWNER TO emhealth;
        ALTER TABLE uec.error_definitions OWNER TO emhealth;
        ALTER TABLE uec.errors OWNER TO emhealth;

        -- 4. New events schema
        CREATE SCHEMA IF NOT EXISTS events AUTHORIZATION emhealth;
        COMMENT ON SCHEMA events IS 'HM events';

        -- 5. move tables (triggers move automatically)
        ALTER TABLE public.instruments SET SCHEMA events;
        ALTER TABLE public.enum_types SET SCHEMA events;
        ALTER TABLE public.enum_values SET SCHEMA events;
        ALTER TABLE public.enum_values_history SET SCHEMA events;
        ALTER TABLE public.parameters SET SCHEMA events;
        ALTER TABLE public.parameters_history SET SCHEMA events;
        ALTER TABLE public.data_staging SET SCHEMA events;
        ALTER TABLE public.data SET SCHEMA events;

        -- 6. move mat. views & CAGGs, update job procedures
        ALTER MATERIALIZED VIEW public.em_off SET SCHEMA events;
        ALTER MATERIALIZED VIEW public.em_off_daily SET SCHEMA events;
        ALTER MATERIALIZED VIEW public.load_counters_daily SET SCHEMA events;

        ALTER MATERIALIZED VIEW events.em_off OWNER TO emhealth;
        ALTER MATERIALIZED VIEW events.em_off_daily OWNER TO emhealth;
        ALTER MATERIALIZED VIEW events.load_counters_daily OWNER TO emhealth;

        ALTER PROCEDURE public.refresh_em_off SET SCHEMA events;
        ALTER PROCEDURE events.refresh_em_off OWNER TO emhealth;

        IF dbname = 'sem' THEN
            ALTER MATERIALIZED VIEW public.sem_beamtime_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.fib_baa_counters_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.fib_bda_counters_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.gis_counters_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.fib_beam_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.sem_beam_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.flm_beam_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.sem_cryocycle_al_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.sem_cryocycle_noal_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.sem_chamber_state_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.sem_pressure_hourly SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.sem_pressure_daily SET SCHEMA events;

            ALTER MATERIALIZED VIEW events.sem_beamtime_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.fib_baa_counters_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.fib_bda_counters_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.gis_counters_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.fib_beam_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.sem_beam_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.flm_beam_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.sem_cryocycle_al_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.sem_cryocycle_noal_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.sem_chamber_state_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.sem_pressure_hourly OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.sem_pressure_daily OWNER TO emhealth;

            ALTER PROCEDURE public.refresh_sem_beamtime_daily SET SCHEMA events;
            ALTER PROCEDURE events.refresh_sem_beamtime_daily OWNER TO emhealth;
        ELSE
            ALTER MATERIALIZED VIEW public.epu_counters SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.epu_running_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.epu_runs SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.epu_sessions SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.epud_runs SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.tomo_counters SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.tomo_runs SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.tomo_running_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.tomo_sessions SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.vacuum_state_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.data_counters_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.image_counters_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.epu_state_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.tomo_state_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.tem_cryocycle_daily SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.tem_pressure_hourly SET SCHEMA events;
            ALTER MATERIALIZED VIEW public.tem_pressure_daily SET SCHEMA events;

            ALTER MATERIALIZED VIEW events.epu_counters OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.epu_running_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.epu_runs OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.epu_sessions OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.epud_runs OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.tomo_counters OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.tomo_runs OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.tomo_running_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.tomo_sessions OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.vacuum_state_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.data_counters_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.image_counters_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.epu_state_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.tomo_state_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.tem_cryocycle_daily OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.tem_pressure_hourly OWNER TO emhealth;
            ALTER MATERIALIZED VIEW events.tem_pressure_daily OWNER TO emhealth;

            ALTER PROCEDURE public.refresh_epu_sessions SET SCHEMA events;
            ALTER PROCEDURE events.refresh_epu_sessions OWNER TO emhealth;

            ALTER PROCEDURE public.refresh_tomo_sessions SET SCHEMA events;
            ALTER PROCEDURE events.refresh_tomo_sessions OWNER TO emhealth;

            ALTER PROCEDURE public.refresh_vacuum_state_daily SET SCHEMA events;
            ALTER PROCEDURE events.refresh_vacuum_state_daily OWNER TO emhealth;

            ALTER PROCEDURE public.refresh_epu_runs SET SCHEMA events;
            ALTER PROCEDURE events.refresh_epu_runs OWNER TO emhealth;

            ALTER PROCEDURE public.refresh_epud_runs SET SCHEMA events;
            ALTER PROCEDURE events.refresh_epud_runs OWNER TO emhealth;

            ALTER PROCEDURE public.refresh_tomo_runs SET SCHEMA events;
            ALTER PROCEDURE events.refresh_tomo_runs OWNER TO emhealth;

            ALTER PROCEDURE public.refresh_epu_counters SET SCHEMA events;
            ALTER PROCEDURE events.refresh_epu_counters OWNER TO emhealth;

            ALTER PROCEDURE public.refresh_tomo_counters SET SCHEMA events;
            ALTER PROCEDURE events.refresh_tomo_counters OWNER TO emhealth;

            ALTER PROCEDURE public.refresh_epu_running_daily SET SCHEMA events;
            ALTER PROCEDURE events.refresh_epu_running_daily OWNER TO emhealth;

            ALTER PROCEDURE public.refresh_tomo_running_daily SET SCHEMA events;
            ALTER PROCEDURE events.refresh_tomo_running_daily OWNER TO emhealth;
        END IF;

        UPDATE _timescaledb_config.bgw_job SET owner = 'emhealth' WHERE proc_schema = 'events' AND proc_name like 'refresh_%';

        -- 7. update FKs
        ALTER TABLE events.enum_types DROP CONSTRAINT enum_types_instrument_id_fkey;
        ALTER TABLE events.enum_types ADD CONSTRAINT enum_types_instrument_id_fkey FOREIGN KEY (instrument_id) REFERENCES events.instruments(id) ON DELETE CASCADE;

        ALTER TABLE events.parameters DROP CONSTRAINT parameters_instrument_id_fkey;
        ALTER TABLE events.parameters ADD CONSTRAINT parameters_instrument_id_fkey FOREIGN KEY (instrument_id) REFERENCES events.instruments(id) ON DELETE CASCADE;

        ALTER TABLE events.parameters_history DROP CONSTRAINT parameters_history_instrument_id_fkey;
        ALTER TABLE events.parameters_history ADD CONSTRAINT parameters_history_instrument_id_fkey FOREIGN KEY (instrument_id) REFERENCES events.instruments(id) ON DELETE CASCADE;

        ALTER TABLE uec.errors DROP CONSTRAINT errors_instrumentid_fkey;
        ALTER TABLE uec.errors ADD CONSTRAINT errors_instrument_id_fkey FOREIGN KEY (instrument_id) REFERENCES events.instruments(id) ON DELETE CASCADE;

        -- 8. update functions
        SELECT EXISTS (
            SELECT 1
            FROM pg_proc p
                     JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = 'public'
              AND p.proname = 'purge_old_chunks'
        ) INTO purge_func_exists;

        IF purge_func_exists THEN
            ALTER FUNCTION public.purge_old_chunks(text, integer, integer) SET SCHEMA events;
        END IF;

        ALTER FUNCTION public.enum_values_upsert_before_insert() SET SCHEMA events;
        ALTER FUNCTION public.parameters_upsert_before_insert() SET SCHEMA events;
        ALTER FUNCTION public.enum_values_log_after_update() SET SCHEMA events;
        ALTER FUNCTION public.parameters_log_after_update() SET SCHEMA events;

        -- 9. update trigger funcs
        EXECUTE $sql$
CREATE OR REPLACE FUNCTION events.enum_values_upsert_before_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $func$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM events.enum_values
        WHERE enum_id = NEW.enum_id
          AND member_name = NEW.member_name
    ) THEN
        UPDATE events.enum_values
        SET value = NEW.value
        WHERE enum_id = NEW.enum_id
          AND member_name = NEW.member_name;

        RETURN NULL; -- skip the insert
    ELSE
        RETURN NEW; -- proceed with insert
    END IF;
END;
$func$;
$sql$;

        EXECUTE $sql$
CREATE OR REPLACE FUNCTION events.parameters_upsert_before_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $func$
BEGIN
    -- If exists, update instead of insert
    IF EXISTS (
        SELECT 1
        FROM events.parameters
        WHERE instrument_id = NEW.instrument_id
          AND param_id = NEW.param_id
    ) THEN
        UPDATE events.parameters
        SET subsystem = NEW.subsystem,
            component = NEW.component,
            param_name = NEW.param_name,
            display_name = NEW.display_name,
            display_unit = NEW.display_unit,
            storage_unit = NEW.storage_unit,
            enum_id = NEW.enum_id,
            value_type = NEW.value_type,
            event_id = NEW.event_id,
            event_name = NEW.event_name,
            abs_min = NEW.abs_min,
            abs_max = NEW.abs_max
        WHERE instrument_id = NEW.instrument_id
          AND param_id = NEW.param_id;
        RETURN NULL; -- skip insert
    ELSE
        RETURN NEW; -- proceed with insert
    END IF;
END;
$func$;
$sql$;

        EXECUTE $sql$
CREATE OR REPLACE FUNCTION events.enum_values_log_after_update()
RETURNS trigger
LANGUAGE plpgsql
AS $func$
BEGIN
    IF ROW(OLD.*) IS DISTINCT FROM ROW(NEW.*) THEN
        INSERT INTO events.enum_values_history (enum_id, member_name, value)
        VALUES (OLD.enum_id,OLD.member_name, OLD.value);
        RAISE NOTICE 'Updated enum_values for enum_id %', OLD.enum_id;
    END IF;

    RETURN NEW;
END;
$func$;
$sql$;

        EXECUTE $sql$
CREATE OR REPLACE FUNCTION events.parameters_log_after_update()
RETURNS trigger
LANGUAGE plpgsql
AS $func$
BEGIN
IF ROW(OLD.*) IS DISTINCT FROM ROW(NEW.*) THEN
    INSERT INTO events.parameters_history (
        instrument_id, param_id, subsystem, component, param_name, display_name,
        display_unit, storage_unit, enum_id, value_type, event_id, event_name,
        abs_min, abs_max
    )
    VALUES (
        OLD.instrument_id, OLD.param_id, OLD.subsystem, OLD.component, OLD.param_name, OLD.display_name,
        OLD.display_unit, OLD.storage_unit, OLD.enum_id, OLD.value_type, OLD.event_id, OLD.event_name,
        OLD.abs_min, OLD.abs_max
    );

    RAISE NOTICE 'Updated parameter % (instrument %)', NEW.param_id, NEW.instrument_id;
END IF;

RETURN NEW;
END;
$func$
$sql$;

        -- 10. change owner
        ALTER TABLE events.instruments OWNER TO emhealth;
        ALTER TABLE events.enum_types OWNER TO emhealth;
        ALTER TABLE events.enum_values OWNER TO emhealth;
        ALTER TABLE events.enum_values_history OWNER TO emhealth;
        ALTER TABLE events.parameters OWNER TO emhealth;
        ALTER TABLE events.parameters_history OWNER TO emhealth;
        ALTER TABLE events.data_staging OWNER TO emhealth;
        ALTER TABLE events.data OWNER TO emhealth;

        -- 11. pganalyze needs extra access
        GRANT pg_read_server_files TO pganalyze;
        GRANT EXECUTE ON FUNCTION pg_read_file(text, bigint, bigint) TO pganalyze;
        GRANT USAGE ON SCHEMA events, uec TO pganalyze;
        GRANT SELECT ON ALL TABLES IN SCHEMA events, uec TO pganalyze;
        ALTER DEFAULT PRIVILEGES FOR ROLE emhealth IN SCHEMA events, uec GRANT SELECT ON TABLES TO pganalyze;

        -- 12. grafana permissions
        GRANT USAGE ON SCHEMA events, uec, pganalyze TO grafana;
        GRANT SELECT ON ALL TABLES IN SCHEMA events, uec, pganalyze TO grafana;
        ALTER DEFAULT PRIVILEGES FOR ROLE emhealth IN SCHEMA events, uec GRANT SELECT ON TABLES TO grafana;
        ALTER DEFAULT PRIVILEGES FOR ROLE pganalyze IN SCHEMA pganalyze GRANT SELECT ON TABLES TO grafana;

        -- 13. Update search path
        ALTER ROLE emhealth SET search_path = events,uec,public;
        ALTER ROLE pganalyze SET search_path = pganalyze,public;
        ALTER ROLE grafana SET search_path = events,uec,pganalyze,public;

        -- 14. Update schema version
        COMMENT ON TABLE public.schema_info IS 'Global schema version';
        GRANT SELECT ON TABLE public.schema_info TO PUBLIC;
        UPDATE public.schema_info SET version = 5;
    END IF;
END $$
