-- Create a CAGG of FIB BAA erosion time counters
CREATE MATERIALIZED VIEW events.fib_baa_counters_daily WITH (timescaledb.continuous) AS
  SELECT
    d.instrument_id,
    time_bucket('1 day', d.time) AS day,
    delta(counter_agg(d.time, d.value_num)) AS daily_erosion,
    (SUBSTRING(p.param_name FROM '(\d+)$')::INT + 1) AS aperture_idx
  FROM
    events.data d
    JOIN events.parameters p
      USING (param_id, instrument_id)
  WHERE
    p.subsystem = 'AvaBaa'
  GROUP BY
    d.instrument_id,
    day,
    aperture_idx
WITH NO DATA