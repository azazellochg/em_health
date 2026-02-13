-- Create a CAGG of FIB BAA erosion time counters
CREATE MATERIALIZED VIEW fib_baa_counters_daily WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 day', d.time) AS day,
        d.instrument_id,
        delta(counter_agg(d.time, d.value_num)) AS daily_erosion,
        (substring(p.param_name FROM '(\d+)$')::int + 1) AS aperture_idx
    FROM
        data d
        JOIN parameters p USING (param_id, instrument_id)
    WHERE p.subsystem = 'AvaBaa'
    GROUP BY day, d.instrument_id, aperture_idx
 WITH NO DATA


