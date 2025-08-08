DO $$
    DECLARE
        current_version INTEGER;
    BEGIN
        SELECT version INTO current_version FROM public.schema_info LIMIT 1;

        IF current_version = 2 THEN

-- remove dbid from pganalyze.stat_statements, also remove userid from the pkey
            SELECT remove_compression_policy('pganalyze.stat_statements');
            SELECT decompress_chunk(chunk_table)
            FROM show_chunks('pganalyze.stat_statements') AS chunk_table;

            ALTER TABLE pganalyze.stat_statements
                DROP CONSTRAINT stat_statements_pkey,
                DROP COLUMN dbid,
                DROP COLUMN userid,
                ADD PRIMARY KEY (collected_at, queryid),
                SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'queryid',
                    timescaledb.compress_orderby = 'collected_at DESC');

            SELECT add_compression_policy('pganalyze.stat_statements', INTERVAL :TBL_STATEMENTS_COMPRESSION);

-- remove datname from pganaluze.vacuum stats
            ALTER TABLE pganalyze.vacuum_stats
                DROP COLUMN datname,
                DROP CONSTRAINT vacuum_stats_pkey,
                ADD PRIMARY KEY (schemaname, tablename, started_at);

-- remove db columns from pganalyze.database_stats
            ALTER TABLE pganalyze.database_stats
                DROP COLUMN datname,
                DROP COLUMN datid;

            UPDATE public.schema_info SET version = 3;

        END IF;
    END $$;
