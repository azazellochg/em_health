DO $$
DECLARE
    current_version INTEGER;
    job INTEGER;
    chunk regclass;
BEGIN
    -- Get current schema version
    SELECT MAX(version) INTO current_version FROM public.schema_info;

    IF current_version = 5 THEN
        -- 1. Change segment_by for events/data. (instrument_id,param_id) gives <100 rows/chunk so it's way too granular
        SELECT job_id INTO job
        FROM timescaledb_information.jobs
        WHERE proc_name = 'policy_compression'
          AND hypertable_schema = 'events'
          AND hypertable_name = 'data';

        PERFORM alter_job(job, scheduled => false);

        FOR chunk IN SELECT show_chunks('events.data') LOOP
            CALL convert_to_rowstore(chunk);
        END LOOP;

        ALTER TABLE events.data SET (timescaledb.segmentby = 'instrument_id', timescaledb.orderby = 'time DESC');

        PERFORM alter_job(job, scheduled => true);

        -- 2. Make pganalyze.stat_snapshots a normal table
        SET ROLE pganalyze;
        ALTER TABLE pganalyze.stat_snapshots RENAME TO stat_snapshots_old;
        CREATE TABLE IF NOT EXISTS pganalyze.stat_snapshots (
                                           collected_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
                                           calls                   BIGINT      NOT NULL,
                                           total_plan_time         DOUBLE PRECISION NOT NULL,
                                           total_exec_time         DOUBLE PRECISION NOT NULL,
                                           rows                    BIGINT      NOT NULL,
                                           shared_blks_hit         BIGINT      NOT NULL,
                                           shared_blks_read        BIGINT      NOT NULL,
                                           shared_blks_dirtied     BIGINT      NOT NULL,
                                           shared_blks_written     BIGINT      NOT NULL,
                                           local_blks_hit          BIGINT      NOT NULL,
                                           local_blks_read         BIGINT      NOT NULL,
                                           local_blks_dirtied      BIGINT      NOT NULL,
                                           local_blks_written      BIGINT      NOT NULL,
                                           temp_blks_read          BIGINT      NOT NULL,
                                           temp_blks_written       BIGINT      NOT NULL,
                                           blk_read_time           DOUBLE PRECISION NOT NULL,
                                           blk_write_time          DOUBLE PRECISION NOT NULL,
                                           wal_records             BIGINT      NOT NULL,
                                           wal_fpi                 BIGINT      NOT NULL,
                                           wal_bytes               NUMERIC     NOT NULL,
                                           stats_reset             TIMESTAMPTZ NOT NULL
        );

        INSERT INTO pganalyze.stat_snapshots
        SELECT *
        FROM pganalyze.stat_snapshots_old
        ORDER BY collected_at;

        SET ROLE postgres;
        DROP TABLE pganalyze.stat_snapshots_old;

        -- 3. Update pganalyze.purge_stats func
        PERFORM delete_job(job_id) FROM timescaledb_information.jobs WHERE proc_name='purge_stats';
        EXECUTE $sql$
DROP FUNCTION IF EXISTS pganalyze.purge_stats;
CREATE FUNCTION pganalyze.purge_stats(job_id int = NULL, config jsonb = '{"drop_after":"3 months"}')
RETURNS void
LANGUAGE plpgsql
AS $func$
DECLARE
    drop_after interval;
BEGIN
    SELECT jsonb_object_field_text (config, 'drop_after')::interval
    INTO STRICT drop_after;

    IF drop_after IS NULL THEN
        RAISE EXCEPTION 'Config must have drop_after';
    END IF;

    DELETE FROM pganalyze.database_stats
    WHERE collected_at < NOW() - drop_after;

    DELETE FROM pganalyze.table_stats
    WHERE collected_at < NOW() - drop_after;

    DELETE FROM pganalyze.index_stats
    WHERE collected_at < NOW() - drop_after;

    DELETE FROM pganalyze.vacuum_stats
    WHERE started_at < NOW() - drop_after;

    DELETE FROM pganalyze.stat_explains
    WHERE time < NOW() - drop_after;

    DELETE FROM pganalyze.stat_snapshots
    WHERE collected_at < NOW() - drop_after;

    DELETE FROM pganalyze.sys_stats
    WHERE time < NOW() - drop_after;
END;
$func$;
$sql$;

        PERFORM add_job('pganalyze.purge_stats', schedule_interval=>'1 day'::interval, config => '{"drop_after":"3 months"}');

        -- 4. Update schema version
        UPDATE public.schema_info SET version = 6;
    END IF;
END $$
