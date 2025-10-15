/* Create a CAGG of acquired data counter
   (Tb per day). Only Falcon cameras have such a counter.
*/
CREATE MATERIALIZED VIEW data_counters_daily
WITH (timescaledb.continuous)
    AS
SELECT
    time_bucket('1 day', d.time) AS day,
    d.instrument_id,
    p.param_name,
    MAX(d.value_num) - MIN(d.value_num) AS daily_terabytes
FROM data d
         JOIN parameters p
              ON d.param_id = p.param_id AND d.instrument_id = p.instrument_id
WHERE p.param_name IN ('NumberOffloadedTerabytes', 'BM-Falcon-NumberOffloadedTB')
GROUP BY day, d.instrument_id, p.param_name
WITH NO DATA