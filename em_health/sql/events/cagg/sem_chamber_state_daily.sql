/* Create a CAGG of all SEM chamber states other than Pumped */
CREATE MATERIALIZED VIEW events.sem_chamber_state_daily WITH (timescaledb.continuous) AS
  SELECT
    d.instrument_id,
    time_bucket('1 day', d.time) AS day,
    toolkit_experimental.compact_state_agg(d.time, (d.value_num::INT != e.value)::INT) AS agg
  FROM
    events.data d
    JOIN events.parameters p
      USING (instrument_id, param_id)
    LEFT JOIN events.enum_values e
      ON e.enum_id = p.enum_id
  WHERE
    p.param_name = 'Chamber.StateSC'
    AND p.component = 'Chamber'
    AND e.member_name = 'Pumped'
  GROUP BY
    d.instrument_id,
    p.param_name,
    day
WITH NO DATA