/* Create a CAGG of Tomo acquisition states
   Here we ignore the session data and aggregate the acquisition state directly from raw data.
   The end goal is to have states duration per day, not per session.
 */
CREATE MATERIALIZED VIEW tomo_state_daily
WITH (timescaledb.continuous) AS
SELECT
    d.instrument_id,
    time_bucket('1 day', d.time) AS time,
    toolkit_experimental.compact_state_agg(d.time, d.value_num::bigint) AS agg
FROM data d
JOIN parameters p
  ON d.instrument_id = p.instrument_id
 AND d.param_id = p.param_id
WHERE p.param_name IN ('Tomo5TiltSeriesState', 'TiltSeries')
  AND p.subsystem = 'Tomography'
GROUP BY d.instrument_id, time_bucket('1 day', d.time)
WITH NO DATA;
