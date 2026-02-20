/* Create a CAGG of SEM cryo cycle state
Non-autoloader systems: track when temperature > 0
   */
CREATE MATERIALIZED VIEW sem_cryocycle_noal_daily WITH (timescaledb.continuous) AS
SELECT
    d.instrument_id,
    time_bucket('1 day', d.time) AS day,
    toolkit_experimental.compact_state_agg(d.time, (d.value_num > 0)::int) AS agg
FROM data d
         JOIN parameters p USING (instrument_id, param_id)
WHERE p.param_name = 'ExperimentalThermometerCryoShieldTemperature'
GROUP BY d.instrument_id, day
WITH NO DATA