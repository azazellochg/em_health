/* Create a CAGG of TEM cryo cycles.
   The duration returned is always based on the temperature algorithm
   (if the conditioning is set to "delayed", e.g. 12h, the returned value will still be only a few hours).
*/
CREATE MATERIALIZED VIEW events.tem_cryocycle_daily WITH (timescaledb.continuous) AS
SELECT
    d.instrument_id,
    time_bucket('1 day', d.time) AS day,
    REPLACE(p.param_name, 'CryoCycleDuration', '') AS param_name,
    state_agg(d.time, (d.value_num>0)::int) AS agg
FROM events.data d
         JOIN events.parameters p USING (instrument_id, param_id)
WHERE p.param_name IN ('AutoloaderCryoCycleDuration', 'ColumnCryoCycleDuration', 'CryoCycleDurationCLS', 'CryoCycleDurationMicroscope')
GROUP BY d.instrument_id, p.param_name, day
WITH NO DATA