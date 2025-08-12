-- Create a materialized view of query statistics
CREATE MATERIALIZED VIEW pganalyze.stat_statements_cagg
            WITH (timescaledb.continuous)
AS
SELECT
    time_bucket('1 minute', collected_at) AS bucket,
    queryid,
    MAX(query) AS query,
    SUM(calls) AS calls,
    SUM(total_time) AS total_time,
    SUM(blk_read_time) AS blk_read_time,
    SUM(blk_write_time) AS blk_write_time
FROM pganalyze.stat_statements
GROUP BY bucket, queryid
WITH NO DATA