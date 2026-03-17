-- Create a CAGG of FIB BDA erosion time counters
CREATE MATERIALIZED VIEW fib_bda_counters_daily WITH (timescaledb.continuous) AS
SELECT
    d.instrument_id,
    time_bucket('1 day', d.time) AS day,
    delta(counter_agg(d.time, d.value_num)) AS daily_erosion,
    (substring(p.param_name FROM '(\d+)$')::int + 1) AS aperture_idx
FROM
    data d
        JOIN parameters p USING (param_id, instrument_id)
WHERE p.subsystem IN ('AvaBda', 'AvaFib')
  AND p.component ILIKE '%strip%'
GROUP BY d.instrument_id, day, aperture_idx
WITH NO DATA