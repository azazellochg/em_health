/* Create a view of the valve open time (s) and the counter value (s) of each GIS port per day
   from the cagg events.gis_counter_values_daily.
   A cagg sees only the points of its own bucket, so the increment between the last point of a day
   and the first point of the next day is not visible inside either day:
   the view adds the increment from the last value of the previous day with data.
   A counter drop (reset) is not counted.
*/
CREATE VIEW events.gis_counters_daily AS
  SELECT
    instrument_id,
    day,
    gis_port,
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
      + GREATEST(first_value - LAG(last_value) OVER (PARTITION BY instrument_id, gis_port ORDER BY day), 0)
    ) AS daily_usage,
    -- counter value at the end of the day
    last_value AS total_usage
  FROM events.gis_counter_values_daily
