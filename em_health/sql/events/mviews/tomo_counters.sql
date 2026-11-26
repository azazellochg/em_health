/* Create a materialized view of Tomo session counters:
   images, tilt series and search maps. Sessions with 0 images are removed.
   TemImageCount counts the images of a tilt series and starts at 1 again for the next one.
   TiltSeriesNumberOfPositions (current position of the batch) and SearchMapsPerJob count up and go back to 0
   when the batch or job is done, sometimes the next one starts without a 0.
   All three are the sum of the counter increments: value - previous value, or the value itself after a drop
   (a new tilt series, batch or job). Counting the last value before each 0 lost the last batch when its 0 was
   logged with the session close, and the value before a drop to a non-zero value.

      Depends on tomo_sessions view
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS events.tomo_counters AS
  WITH
    counter_params AS (
      SELECT
        instrument_id,
        param_id,
        param_name
      FROM
        events.parameters
      WHERE
        param_name IN ('TemImageCount', 'TiltSeriesNumberOfPositions', 'SearchMapsPerJob')
        AND subsystem = 'Tomography'
    )
  SELECT
    seg.instrument_id,
    seg.session_id,
    seg.start_time,
    agg.total_image_counter::INT,
    agg.total_ts_counter::INT,
    agg.total_sm_counter::INT
  FROM
    events.tomo_sessions seg
    JOIN LATERAL ( SELECT
                     SUM(i.increment) FILTER (WHERE cp.param_name = 'TemImageCount') AS total_image_counter,
                     COALESCE(SUM(i.increment) FILTER (WHERE cp.param_name = 'TiltSeriesNumberOfPositions'), 0)
                       AS total_ts_counter,
                     COALESCE(SUM(i.increment) FILTER (WHERE cp.param_name = 'SearchMapsPerJob'), 0)
                       AS total_sm_counter
                   FROM
                     counter_params cp
                     -- LATERAL per parameter: TimescaleDB skips compressed batches of other parameters
                     CROSS JOIN LATERAL ( SELECT
                                            CASE WHEN p.value_num >= p.prev_value THEN p.value_num - p.prev_value
                                                 ELSE p.value_num END AS increment
                                          FROM
                                            ( SELECT
                                                d.value_num,
                                                LAG(d.value_num) OVER (ORDER BY d.time) AS prev_value
                                              FROM
                                                events.data d
                                              WHERE
                                                d.instrument_id = cp.instrument_id
                                                AND d.param_id = cp.param_id
                                                AND d.time >= seg.start_time
                                                AND d.time < seg.end_time ) p ) i
                   WHERE
                     cp.instrument_id = seg.instrument_id ) agg
      ON TRUE
  WHERE
    agg.total_image_counter > 0
  ORDER BY
    seg.instrument_id,
    seg.session_id
