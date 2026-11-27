/* Create a view of the erosion time (s) of each FIB BAA aperture per day from the cagg
   events.fib_baa_counter_values_daily.
   A cagg sees only the points of its own bucket, so the increment between the last point of a day
   and the first point of the next day is not visible inside either day:
   the view adds the increment from the last value of the previous day with data.
   A counter drop (reset) is not counted: the drop to the new strip's values is not usage.
*/
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
