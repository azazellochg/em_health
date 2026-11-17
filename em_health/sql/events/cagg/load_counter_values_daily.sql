/* Create a CAGG of autoloader counter values per day, and the view events.load_counters_daily
   with the number of loads per day.
   A cagg sees only the points of its own bucket, so a load whose new counter value is the first
   point of a day is not visible inside that day: the view adds the increment from the last value
   of the previous day with data. A counter drop (reset or restore, e.g. 12051 -> 9847) is not a load.
*/
-- load_counters_daily was a cagg before: replace it with the view
DROP MATERIALIZED VIEW IF EXISTS events.load_counters_daily;

CREATE MATERIALIZED VIEW events.load_counter_values_daily WITH (timescaledb.continuous) AS
  SELECT
    time_bucket('1 day', d.time) AS day,
    d.instrument_id,
    CASE p.param_name WHEN 'LoadCassetteCounter' THEN 'cassette' ELSE 'cartridge' END AS counter,
    first(d.value_num, d.time) AS first_value,
    last(d.value_num, d.time) AS last_value,
    MIN(d.value_num) AS min_value,
    MAX(d.value_num) AS max_value,
    num_resets(counter_agg(d.time, d.value_num)) AS num_drops
  FROM
    events.data d
    JOIN events.parameters p
      USING (param_id, instrument_id)
  WHERE
    p.param_name IN ('LoadCartridgeCounter', 'LoadCassetteCounter', 'SampleTransferCount')
  GROUP BY
    day,
    d.instrument_id,
    counter
WITH NO DATA;

CREATE VIEW events.load_counters_daily AS
  WITH loads AS (
    SELECT
      day,
      instrument_id,
      counter,
      -- loads within the day; with a drop (counter set to a lower value): the increments
      -- before it (max - first) and after it (last - min)
      CASE
        WHEN num_drops = 0 THEN last_value - first_value
        ELSE (max_value - first_value) + (last_value - min_value)
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
