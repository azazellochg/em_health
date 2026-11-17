/* Create a CAGG of GIS valve open time counter values per day, and the view events.gis_counters_daily
   with the valve open time (s) and the counter value (s) of each GIS port per day.
   A cagg sees only the points of its own bucket, so the increment between the last point of a day
   and the first point of the next day is not visible inside either day: the view adds the increment
   from the last value of the previous day with data. A counter drop (reset) is not counted.
   Zeros are ignored: the counter sometimes logs a single 0 and then returns to its value.
   Some instruments store the counter in hours: values are converted to seconds.
*/
-- gis_counters_daily was a cagg before: replace it with the view
DROP MATERIALIZED VIEW IF EXISTS events.gis_counters_daily;

CREATE MATERIALIZED VIEW events.gis_counter_values_daily WITH (timescaledb.continuous) AS
  SELECT
    d.instrument_id,
    time_bucket('1 day', d.time) AS day,
    p.component AS gis_port,
    first(d.value_num, d.time) * MAX(CASE p.storage_unit WHEN 'h' THEN 3600 ELSE 1 END) AS first_value,
    last(d.value_num, d.time) * MAX(CASE p.storage_unit WHEN 'h' THEN 3600 ELSE 1 END) AS last_value,
    MIN(d.value_num) * MAX(CASE p.storage_unit WHEN 'h' THEN 3600 ELSE 1 END) AS min_value,
    MAX(d.value_num) * MAX(CASE p.storage_unit WHEN 'h' THEN 3600 ELSE 1 END) AS max_value
  FROM
    events.data d
    JOIN events.parameters p
      USING (param_id, instrument_id)
  WHERE
    p.subsystem = 'GISes'
    AND p.param_name LIKE 'GIS%ValveOpenTimeParameter'
    AND d.value_num > 0
  GROUP BY
    d.instrument_id,
    day,
    gis_port
WITH NO DATA;

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
