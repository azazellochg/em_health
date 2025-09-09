CREATE SCHEMA IF NOT EXISTS pganalyze;

-- Create tables
CREATE TABLE pganalyze.database_stats (
                                          collected_at    TIMESTAMPTZ      DEFAULT now() PRIMARY KEY,
                                          xact_commit     BIGINT           NOT NULL,
                                          xact_rollback   BIGINT           NOT NULL,
                                          blks_read       BIGINT           NOT NULL,
                                          blks_hit        BIGINT           NOT NULL,
                                          tup_inserted    BIGINT           NOT NULL,
                                          tup_updated     BIGINT           NOT NULL,
                                          tup_deleted     BIGINT           NOT NULL,
                                          tup_fetched     BIGINT           NOT NULL,
                                          tup_returned    BIGINT           NOT NULL,
                                          temp_files      BIGINT           NOT NULL,
                                          temp_bytes      BIGINT           NOT NULL,
                                          deadlocks       BIGINT           NOT NULL,
                                          blk_read_time   DOUBLE PRECISION NOT NULL,
                                          blk_write_time  DOUBLE PRECISION NOT NULL,
                                          frozen_xid_age  BIGINT           NOT NULL,
                                          frozen_mxid_age BIGINT           NOT NULL,
                                          db_size         BIGINT           NOT NULL
);

CREATE TABLE pganalyze.table_stats (
                                       collected_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                                       relid           OID         NOT NULL,
                                       table_bytes     BIGINT      NOT NULL,
                                       index_bytes     BIGINT      NOT NULL,
                                       toast_bytes     BIGINT      NOT NULL,
                                       frozen_xid_age  BIGINT      NOT NULL,
                                       num_dead_rows   BIGINT      NOT NULL,
                                       num_live_rows   BIGINT      NOT NULL,
                                       PRIMARY KEY (relid, collected_at)
);

CREATE TABLE pganalyze.index_stats (
                                       collected_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
                                       indexrelid           OID         NOT NULL,
                                       relid                OID         NOT NULL,
                                       size_bytes           BIGINT      NOT NULL,
                                       scan                 BIGINT      NOT NULL,
                                       tup_read             BIGINT      NOT NULL,
                                       tup_fetch            BIGINT      NOT NULL,
                                       blks_read            BIGINT      NOT NULL,
                                       blks_hit             BIGINT      NOT NULL,
                                       exclusively_locked   BOOLEAN     NOT NULL,
                                       PRIMARY KEY (indexrelid, collected_at)
);

CREATE TABLE pganalyze.vacuum_stats (
                                        relid                   OID         NOT NULL,
                                        started_at              TIMESTAMPTZ NOT NULL,
                                        finished_at             TIMESTAMPTZ NOT NULL,
                                        index_scans             BIGINT      NOT NULL,
                                        pages_removed           BIGINT      NOT NULL,
                                        tuples_removed          BIGINT      NOT NULL,
                                        tuples_remain           BIGINT      NOT NULL,
                                        wraparound              BOOLEAN     NOT NULL,
                                        details                 TEXT        NOT NULL,
                                        PRIMARY KEY (relid, started_at)
);

CREATE TABLE pganalyze.stat_statements (
                                           collected_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
                                           queryid                 BIGINT      NOT NULL,
                                           query                   TEXT        NOT NULL,
                                           calls                   BIGINT      NOT NULL,
                                           total_time              DOUBLE PRECISION NOT NULL,
                                           min_time                DOUBLE PRECISION NOT NULL,
                                           max_time                DOUBLE PRECISION NOT NULL,
                                           mean_time               DOUBLE PRECISION NOT NULL,
                                           stddev_time             DOUBLE PRECISION NOT NULL,
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
                                           PRIMARY KEY (collected_at, queryid)
) WITH (
                                             tsdb.hypertable,
                                             tsdb.chunk_interval='2 days',
                                             tsdb.partition_column='collected_at',
                                             tsdb.segmentby='queryid',
                                             tsdb.orderby='collected_at ASC',
                                             tsdb.create_default_indexes=false
                                             );

CREATE INDEX IF NOT EXISTS stat_statements_queryid_time ON pganalyze.stat_statements (queryid, collected_at ASC);

SELECT add_retention_policy('pganalyze.stat_statements', drop_after => INTERVAL :TBL_STATS_RETENTION);

CREATE TABLE pganalyze.stat_explains (
                                         time           TIMESTAMPTZ NOT NULL,
                                         queryid        BIGINT      NOT NULL,
                                         duration       DOUBLE PRECISION NOT NULL,
                                         total_cost     DOUBLE PRECISION NOT NULL,
                                         bytes_read     BIGINT      NOT NULL,
                                         io_read_time   DOUBLE PRECISION NOT NULL,
                                         plan           JSON        NOT NULL,
                                         PRIMARY KEY (time, queryid)
);
