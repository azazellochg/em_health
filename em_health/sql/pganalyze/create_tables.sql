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

CREATE TABLE pganalyze.stat_snapshots (
                                           collected_at            TIMESTAMPTZ DEFAULT now() PRIMARY KEY,
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
                                           wal_position            BIGINT      NOT NULL,
                                           stats_reset             TIMESTAMPTZ NOT NULL
) WITH (
                                             tsdb.hypertable,
                                             tsdb.chunk_interval=:var_pgsnaps_chunk_size,
                                             tsdb.partition_column='collected_at',
                                             tsdb.orderby='collected_at'
                                             );

CALL add_columnstore_policy('pganalyze.stat_snapshots', after => INTERVAL :var_pgstats_compression);
SELECT add_retention_policy('pganalyze.stat_snapshots', drop_after => INTERVAL :var_pgstats_retention);

CREATE TABLE IF NOT EXISTS pganalyze.queries (
                                                 queryid BIGINT NOT NULL PRIMARY KEY,
                                                 query TEXT
);

CREATE TABLE pganalyze.stat_statements (
                                           collected_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
                                           userid                  OID         NOT NULL,
                                           queryid                 BIGINT NOT NULL REFERENCES pganalyze.queries(queryid) ON DELETE CASCADE,
                                           plans                   BIGINT      NOT NULL,
                                           calls                   BIGINT      NOT NULL,
                                           total_plan_time         DOUBLE PRECISION NOT NULL,
                                           total_exec_time         DOUBLE PRECISION NOT NULL,
                                           mean_exec_time          DOUBLE PRECISION NOT NULL,
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
                                           wal_bytes               BIGINT      NOT NULL DEFAULT 0,
                                           PRIMARY KEY (queryid, userid, collected_at)
                                           -- To define an index as a UNIQUE or PRIMARY KEY index, the index must include the time column and the partitioning column
) WITH (
                                             tsdb.hypertable,
                                             tsdb.chunk_interval=:var_pgstats_chunk_size,
                                             tsdb.partition_column='collected_at',
                                             tsdb.segmentby='queryid',
                                             tsdb.orderby='collected_at'
                                             );

CALL add_columnstore_policy('pganalyze.stat_statements', after => INTERVAL :var_pgstats_compression);
SELECT add_retention_policy('pganalyze.stat_statements', drop_after => INTERVAL :var_pgstats_retention);

CREATE TABLE pganalyze.stat_explains (
                                         time           TIMESTAMPTZ NOT NULL,
                                         queryid        BIGINT NOT NULL REFERENCES pganalyze.queries(queryid) ON DELETE CASCADE,
                                         duration       DOUBLE PRECISION NOT NULL,
                                         total_cost     DOUBLE PRECISION NOT NULL,
                                         bytes_read     BIGINT      NOT NULL,
                                         io_read_time   DOUBLE PRECISION NOT NULL,
                                         plan           JSON        NOT NULL,
                                         PRIMARY KEY (time, queryid)
);

CREATE TABLE pganalyze.sys_stats (
                                     time       TIMESTAMPTZ NOT NULL DEFAULT now(),
                                     load1      DOUBLE PRECISION NOT NULL,
                                     load5      DOUBLE PRECISION NOT NULL,
                                     load15     DOUBLE PRECISION NOT NULL,
                                     cpu_count  INT         NOT NULL,
                                     mem_total  BIGINT      NOT NULL,
                                     mem_free   BIGINT      NOT NULL,
                                     mem_avail  BIGINT      NOT NULL
);
