/* Create a CAGG of autoloader counter values per day (used by the view events.load_counters_daily). */
CREATE MATERIALIZED VIEW events.load_counter_values_daily WITH (timescaledb.continuous) AS
  SELECT
    time_bucket('1 day', d.time) AS day,
    d.instrument_id,
    CASE p.param_name WHEN 'LoadCassetteCounter' THEN 'cassette' ELSE 'cartridge' END AS counter,
    first(d.value_num, d.time) AS first_value,
    last(d.value_num, d.time) AS last_value,
    MIN(d.value_num) AS min_value,
    MAX(d.value_num) AS max_value
  FROM
    events.data d
    JOIN events.parameters p
      USING (param_id, instrument_id)
  WHERE
    p.param_name IN ('LoadCartridgeCounter', 'LoadCassetteCounter', 'SampleTransferCount')
    AND d.value_num <> 0
  GROUP BY
    day,
    d.instrument_id,
    counter
WITH NO DATA
