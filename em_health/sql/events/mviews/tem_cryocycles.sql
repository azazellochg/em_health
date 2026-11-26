/* Create a materialized view of TEM cryo cycles (Autoloader, Column, and CLS, Microscope on the
   SemiAutomatedLoader).
   The *CryoCycleDuration counter is the time since the cycle start: seconds, but minutes on the
   SemiAutomatedLoader. A cycle starts with a non-zero value after a 0 or after a drop (a new cycle without
   a logged 0); its start is time - counter. The counter is logged every few seconds on most instruments, but
   only every minute (SemiAutomatedLoader) or every hour on some: the cycle ends at the next point (reset to 0
   or a new cycle), but not later than one logging step after its last point (a 0 is also logged every 12 h
   when nothing changes, long after the cycle ended).
   A cycle is complete when a point after it is logged. A cycle without one is still going on (or its reset is
   not logged yet) and ends at its last point.
   Cycles shorter than 1 h are aborted starts and are left out: cryo cycles last a few hours.
   The duration is always based on the temperature algorithm
   (if the conditioning is set to "delayed", e.g. 12h, the counter can still stop after a few hours).
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS events.tem_cryocycles AS
  WITH
    counter_params AS (
      SELECT
        instrument_id,
        param_id,
        REPLACE(param_name, 'CryoCycleDuration', '') AS param_name,
        CASE WHEN param_name IN ('CryoCycleDurationCLS', 'CryoCycleDurationMicroscope') THEN 60 ELSE 1 END
          AS unit_seconds
      FROM
        events.parameters
      WHERE
        param_name IN
        ('AutoloaderCryoCycleDuration', 'ColumnCryoCycleDuration', 'CryoCycleDurationCLS', 'CryoCycleDurationMicroscope')
    ),

    -- LATERAL per parameter: TimescaleDB skips compressed batches of other parameters, OFFSET 0 keeps it a LATERAL
    points AS (
      SELECT
        cp.instrument_id,
        cp.param_id,
        cp.param_name,
        d.time,
        d.value_num,
        d.time - make_interval(secs => d.value_num * cp.unit_seconds) AS cycle_start,
        LAG(d.value_num) OVER w AS prev_value,
        LEAD(d.time) OVER w AS next_time
      FROM
        counter_params cp
        CROSS JOIN LATERAL (
          SELECT
            x.time,
            x.value_num
          FROM
            events.data x
          WHERE
            x.instrument_id = cp.instrument_id
            AND x.param_id = cp.param_id
          OFFSET 0
        ) d
      WINDOW w AS (PARTITION BY cp.instrument_id, cp.param_id ORDER BY d.time)
    ),

    numbered AS (
      SELECT
        *,
        COUNT(*) FILTER (WHERE value_num > 0 AND (prev_value IS NULL OR prev_value = 0 OR value_num < prev_value))
          OVER (PARTITION BY instrument_id, param_id ORDER BY time) AS cycle_id
      FROM
        points
    ),

    cycles AS (
      SELECT
        instrument_id,
        param_name,
        MIN(cycle_start) AS start_time,
        MAX(time) AS last_time,
        -- logging step: the average interval between the points of the cycle
        (MAX(time) - MIN(time)) / NULLIF(COUNT(*) - 1, 0) AS step,
        -- the point after the last point of the cycle
        (ARRAY_AGG(next_time ORDER BY time DESC))[1] AS next_time
      FROM
        numbered
      WHERE
        value_num > 0
      GROUP BY
        instrument_id,
        param_id,
        param_name,
        cycle_id
    ),

    cycle_ends AS (
      SELECT
        instrument_id,
        param_name,
        start_time,
        CASE WHEN next_time IS NULL THEN last_time
             ELSE LEAST(next_time, last_time + COALESCE(step, '0 second'::INTERVAL)) END AS end_time,
        next_time IS NOT NULL AS is_complete
      FROM
        cycles
    )

  SELECT
    instrument_id,
    param_name,
    start_time,
    end_time,
    is_complete
  FROM
    cycle_ends
  WHERE
    end_time - start_time >= '1 hour'::INTERVAL
  ORDER BY
    instrument_id,
    param_name,
    start_time
