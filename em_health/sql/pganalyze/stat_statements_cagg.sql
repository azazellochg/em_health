-- Create a materialized view of query statistics
DROP MATERIALIZED VIEW IF EXISTS pganalyze.stat_statements_cagg;
CREATE MATERIALIZED VIEW pganalyze.stat_statements_cagg
            WITH (timescaledb.continuous)
AS
SELECT
    time_bucket('5 minute', collected_at) AS bucket,
    queryid,
    query,
    -- Counter aggregates (handle deltas automatically)
    counter_agg(collected_at, calls) AS calls,
    counter_agg(collected_at, total_time) AS total_time,
    counter_agg(collected_at, blk_read_time) AS blk_read_time,
    counter_agg(collected_at, blk_write_time) AS blk_write_time,
    counter_agg(collected_at, shared_blks_hit) AS shared_blks_hit,
    counter_agg(collected_at, shared_blks_read) AS shared_blks_read,
    counter_agg(collected_at, rows) AS rows
FROM pganalyze.stat_statements
GROUP BY bucket, queryid, query
WITH NO DATA