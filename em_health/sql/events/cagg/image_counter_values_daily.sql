/* Create a CAGG of acquisition counter values per day, and the view events.image_counters_daily
   with the number of acquisitions per day: AcquisitionJobs, BM-Falcon-NumberOfAcquisitionJobs,
   BM-Ceta-NumberOfAcquisitionJobs and AcquisitionNumber (for Gatan cameras).
   A cagg sees only the points of its own bucket, so the increment between the last point of a day
   and the first point of the next day is not visible inside either day: the view adds the increment
   from the last value of the previous day with data. A counter drop (reset or restore) is not counted.
*/
-- image_counters_daily was a cagg before: replace it with the view
DROP MATERIALIZED VIEW IF EXISTS events.image_counters_daily;

CREATE MATERIALIZED VIEW events.image_counter_values_daily WITH (timescaledb.continuous) AS
  SELECT
    time_bucket('1 day', d.time) AS day,
    d.instrument_id,
    p.param_name,
    first(d.value_num, d.time) AS first_value,
    last(d.value_num, d.time) AS last_value,
    MIN(d.value_num) AS min_value,
    MAX(d.value_num) AS max_value
  FROM
    events.data d
    JOIN events.parameters p
      USING (instrument_id, param_id)
  WHERE
    p.param_name IN ('AcquisitionJobs',
                     'BM-Falcon-NumberOfAcquisitionJobs',
                     'BM-Ceta-NumberOfAcquisitionJobs',
                     'AcquisitionNumber'
      )
    AND d.value_num <> 0
  GROUP BY
    day,
    d.instrument_id,
    p.param_name
WITH NO DATA;

CREATE VIEW events.image_counters_daily AS
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
    )::INT AS daily_images
  FROM events.image_counter_values_daily
