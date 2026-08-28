-- Create a CAGG of autoloader counters
CREATE MATERIALIZED VIEW events.load_counters_daily WITH (timescaledb.continuous) AS
  SELECT
    time_bucket('1 day', d.time) AS day,
    d.instrument_id,

    delta(counter_agg(d.time, d.value_num)
          FILTER (WHERE p.param_name IN ('LoadCartridgeCounter', 'SampleTransferCount')))::INT AS daily_cartridge_count,

    delta(counter_agg(d.time, d.value_num)
          FILTER (WHERE p.param_name = 'LoadCassetteCounter'))::INT AS daily_cassette_count

  FROM
    events.data d
    JOIN events.parameters p
      USING (param_id, instrument_id)
  WHERE
    p.param_name IN ('LoadCartridgeCounter', 'LoadCassetteCounter', 'SampleTransferCount')
  GROUP BY
    day,
    d.instrument_id
WITH NO DATA