/* Create a CAGG of FIB BAA (beam accelerating apertures) erosion time counter values per day, and the view events.fib_baa_counters_daily
   with the erosion time (s) of each aperture per day.
   A cagg sees only the points of its own bucket, so the increment between the last point of a day
   and the first point of the next day is not visible inside either day: the view adds the increment
   from the last value of the previous day with data. A counter drop (reset) is not counted.
   Zeros are ignored: the counter sometimes logs a single 0 and then returns to its value.
*/
-- fib_baa_counters_daily was a cagg before: replace it with the view
DROP MATERIALIZED VIEW IF EXISTS events.fib_baa_counters_daily;

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
WITH NO DATA;

CREATE VIEW events.fib_baa_counters_daily AS
  SELECT
    instrument_id,
    day,
    (
      -- usage within the day. The counter can drop (reset, restore): a cagg does not see the order
      -- of the points, so min and max tell what happened
      CASE
        -- no drop below the first value (a value logged with fewer decimals is a tiny drop)
        WHEN min_value >= first_value THEN last_value - first_value
        -- dropped and stayed below the first value: increments before (max - first) and after (last - min)
        WHEN last_value < first_value THEN (max_value - first_value) + (last_value - min_value)
        -- dropped and grew past the first value: increments after the drop (before it unknown)
        ELSE last_value - min_value
      END
      -- usage between the previous day with data and the first point of this day
      + GREATEST(first_value - LAG(last_value) OVER (PARTITION BY instrument_id, aperture_idx ORDER BY day), 0)
    ) AS daily_erosion,
    aperture_idx
  FROM events.fib_baa_counter_values_daily
