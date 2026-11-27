/* Create a view of the number of autoloader loads per day from the cagg events.load_counter_values_daily.
   A cagg sees only the points of its own bucket, so a load whose new counter value is the first
   point of a day is not visible inside that day:
   the view adds the increment from the last value of the previous day with data.
   A counter drop (reset or restore, e.g. 12051 -> 9847) is not a load.
*/
CREATE VIEW events.load_counters_daily AS
  WITH loads AS (
    SELECT
      day,
      instrument_id,
      counter,
      -- loads within the day. The counter can drop (reset, restore): a cagg does not see the order
      -- of the points, so min and max tell what happened
      CASE
        -- no drop below the first value (a value logged with fewer decimals is a tiny drop)
        WHEN min_value >= first_value THEN last_value - first_value
        -- dropped and stayed below the first value: increments before (max - first) and after (last - min)
        WHEN last_value < first_value THEN (max_value - first_value) + (last_value - min_value)
        -- dropped and grew past the first value: increments after the drop (before it unknown)
        ELSE last_value - min_value
      END
      -- loads between the previous day with data and the first point of this day
      + GREATEST(first_value - LAG(last_value) OVER (PARTITION BY instrument_id, counter ORDER BY day), 0)
      AS loads
    FROM events.load_counter_values_daily
  )
  SELECT
    day,
    instrument_id,
    MAX(loads) FILTER (WHERE counter = 'cartridge')::INT AS daily_cartridge_count,
    MAX(loads) FILTER (WHERE counter = 'cassette')::INT AS daily_cassette_count
  FROM loads
  GROUP BY
    day,
    instrument_id
