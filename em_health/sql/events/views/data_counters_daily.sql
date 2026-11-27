/* Create a view of the offloaded terabytes per day from the cagg events.data_counter_values_daily.
   A cagg sees only the points of its own bucket, so the increment between the last point of a day
   and the first point of the next day is not visible inside either day:
   the view adds the increment from the last value of the previous day with data.
   A counter drop (reset or restore) is not counted.
*/
CREATE VIEW events.data_counters_daily AS
  SELECT
    day,
    instrument_id,
    param_name,
    (
      -- increments within the day. The counter can drop (reset, restore): a cagg does not see the order
      -- of the points, so min and max tell what happened
      CASE
        -- no drop below the first value (a value logged with fewer decimals is a tiny drop)
        WHEN min_value >= first_value THEN last_value - first_value
        -- dropped and stayed below the first value: increments before (max - first) and after (last - min)
        WHEN last_value < first_value THEN (max_value - first_value) + (last_value - min_value)
        -- dropped and grew past the first value: increments after the drop (before it unknown)
        ELSE last_value - min_value
      END
      -- increment between the previous day with data and the first point of this day
      + GREATEST(first_value - LAG(last_value) OVER (PARTITION BY instrument_id, param_name ORDER BY day), 0)
    ) AS daily_terabytes
  FROM events.data_counter_values_daily
