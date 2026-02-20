/* Create a CAGG of SEM cryo cycle state
Autoloader systems: d.value_num = CryoCycle for either Column or AL
   */
CREATE MATERIALIZED VIEW sem_cryocycle_al_daily WITH (timescaledb.continuous) AS
SELECT
    d.instrument_id,
    time_bucket('1 day', d.time) AS day,
    state_agg(d.time, (d.value_num::int = e.value)::int) AS agg
FROM data d
         JOIN parameters p USING (instrument_id, param_id)
         LEFT JOIN enum_values e ON e.enum_id = p.enum_id
WHERE p.param_name LIKE '%TemperatureState'
  AND p.subsystem LIKE '%TemperatureControl'
  AND e.member_name = 'CryoCycle'
GROUP BY d.instrument_id, d.param_id, day
WITH NO DATA