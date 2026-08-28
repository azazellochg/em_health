/* Create a CAGG with "TEM/xT server off" periods.
   Normally, the server value is stored every 2 minutes. If the server goes off,
   the next value will be "1" only when it's up again. So, there are no consecutive zeros.
   It could happen that the server crashed or powered off and there was no 0 recorded.
*/
CREATE MATERIALIZED VIEW events.em_off_daily WITH (timescaledb.continuous) AS
  SELECT
    d.instrument_id,
    time_bucket('1 day', d.time) AS day,
    state_agg(d.time, (d.value_num = 0)::INT) AS agg
  FROM
    events.data d
    JOIN events.parameters p
      USING (instrument_id, param_id)
  WHERE
    p.component = 'Server'
    AND p.param_name = 'Value'
  GROUP BY
    d.instrument_id,
    day
WITH NO DATA
