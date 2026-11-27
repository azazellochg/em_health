/* Create a CAGG of offloaded data counter values per day (used by the view events.data_counters_daily).
   Only TFS cameras have such a counter.
*/
CREATE MATERIALIZED VIEW events.data_counter_values_daily WITH (timescaledb.continuous) AS
  SELECT
    time_bucket('1 day', d.time) AS day,
    d.instrument_id,
    p.param_name,
    first(d.value_num, d.time) AS first_value,
    last(d.value_num, d.time) AS last_value,
    MIN(d.value_num) AS min_value,
    MAX(d.value_num) AS max_value
  FROM
    events.data d
    JOIN events.parameters p
      USING (instrument_id, param_id)
  WHERE
    p.param_name IN ('NumberOffloadedTerabytes',
                     'BM-Falcon-NumberOffloadedTB',
                     'BM-Ceta-NumberOffloadedTB'
      )
    AND d.value_num <> 0
  GROUP BY
    day,
    d.instrument_id,
    p.param_name
WITH NO DATA
