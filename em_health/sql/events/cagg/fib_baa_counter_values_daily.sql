/* Create a CAGG of FIB BAA (beam accelerating apertures) erosion time counter values per day
   (used by the view events.fib_baa_counters_daily).
   Zeros are ignored: when a strip is replaced, all its apertures are reset to 0, and apertures not used
   since then keep logging 0.
*/
CREATE MATERIALIZED VIEW events.fib_baa_counter_values_daily WITH (timescaledb.continuous) AS
  SELECT
    d.instrument_id,
    time_bucket('1 day', d.time) AS day,
    (SUBSTRING(p.param_name FROM '(\d+)$')::INT + 1) AS aperture_idx,
    first(d.value_num, d.time) AS first_value,
    last(d.value_num, d.time) AS last_value,
    MIN(d.value_num) AS min_value,
    MAX(d.value_num) AS max_value
  FROM
    events.data d
    JOIN events.parameters p
      USING (param_id, instrument_id)
  WHERE
    p.subsystem = 'AvaBaa'
    AND d.value_num > 0
  GROUP BY
    d.instrument_id,
    day,
    aperture_idx
WITH NO DATA
