/* Create a CAGG of acquired data counter
   (Tb per day). Only TFS cameras have such a counter.
*/
CREATE MATERIALIZED VIEW data_counters_daily WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 day', d.time) AS day,
        d.instrument_id,
        p.param_name,
        max(d.value_num) - min(d.value_num) AS daily_terabytes
    FROM
        data d
        JOIN parameters p USING (instrument_id, param_id)
    WHERE p.param_name IN ('NumberOffloadedTerabytes', 'BM-Falcon-NumberOffloadedTB', 'BM-Ceta-NumberOffloadedTB')
    GROUP BY day, d.instrument_id, p.param_name
 WITH NO DATA
