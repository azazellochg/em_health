/* Create a CAGG of iFLM running state. Currently Arctis only */
CREATE MATERIALIZED VIEW flm_beam_daily WITH (timescaledb.continuous) AS
SELECT
    d.instrument_id,
    time_bucket('1 day', d.time) AS day,
    toolkit_experimental.compact_state_agg(d.time, (d.value_num::int = e.value)::int) AS agg
FROM data d
         JOIN parameters p USING (instrument_id, param_id)
         LEFT JOIN enum_values e ON e.enum_id = p.enum_id
WHERE p.param_name = 'OpticalMicroscopeStateParameter'
  AND p.subsystem = 'FluorescenceMicroscope'
  AND e.member_name = 'Running'
GROUP BY d.instrument_id, p.param_name, day
WITH NO DATA