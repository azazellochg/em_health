DO $$
DECLARE
    current_version INTEGER;
BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 2 THEN
        -- 1. Remove deprecated mview
        DROP MATERIALIZED VIEW IF EXISTS public.tem_off CASCADE;
        PERFORM delete_job(job_id) FROM timescaledb_information.jobs WHERE proc_name='refresh_tem_off';
        DROP PROCEDURE IF EXISTS public.refresh_tem_off;

        -- 2. Update schema version
        UPDATE public.schema_info SET version = 3;
    END IF;
END $$
