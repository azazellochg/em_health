/* Create a materialized view of FIB/SEM aperture strip replacements: one row per replacement of each strip.
   Each aperture of a strip has an erosion time counter (s). When the strip is replaced, all its apertures are
   reset within seconds: most counters log 0, some only a small value. A drop is a counter going from a non-zero
   value to 0 or below half: apertures not used since a replacement keep logging 0 (no drop), and a value
   restored after a restart is only a little lower. Drops of one strip less than 10 minutes apart are one
   replacement, which needs at least half of the strip's apertures to drop. The replacement time is the first drop.
   max_erosion_time (s) is the erosion time of the most used aperture just before the replacement: the counters
   start from 0 when a strip is installed, so it is known also for a strip installed before the data starts.
   A cagg can't do this: it does not see the value before the first point of a day.
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS events.aperture_strip_replacements AS
  WITH
    strip_params AS (
      SELECT
        instrument_id,
        param_id,
        subsystem,
        component
      FROM
        events.parameters
      WHERE
        component ILIKE '%strip%'
        AND subsystem IN ('AvaBaa', 'AvaBda', 'AvaFib', 'AvaSem')
    ),

    strips AS (
      SELECT
        instrument_id,
        subsystem,
        component,
        COUNT(*) AS apertures
      FROM
        strip_params
      GROUP BY
        instrument_id,
        subsystem,
        component
    ),

    -- LATERAL per parameter: TimescaleDB skips compressed batches of other parameters, OFFSET 0 keeps it a LATERAL
    points AS (
      SELECT
        sp.instrument_id,
        sp.subsystem,
        sp.component,
        sp.param_id,
        d.time,
        d.value_num,
        LAG(d.value_num) OVER (PARTITION BY sp.instrument_id, sp.param_id ORDER BY d.time) AS prev_value
      FROM
        strip_params sp
        CROSS JOIN LATERAL (
          SELECT
            x.time,
            x.value_num
          FROM
            events.data x
          WHERE
            x.instrument_id = sp.instrument_id
            AND x.param_id = sp.param_id
          OFFSET 0
        ) d
    ),

    drops AS (
      SELECT
        instrument_id,
        subsystem,
        component,
        param_id,
        time,
        prev_value,
        LAG(time) OVER (PARTITION BY instrument_id, subsystem, component ORDER BY time) AS prev_drop_time
      FROM
        points
      WHERE
        prev_value > 0
        AND value_num < prev_value / 2
    ),

    grouped AS (
      SELECT
        *,
        COUNT(*) FILTER (WHERE prev_drop_time IS NULL OR time > prev_drop_time + '10 minutes'::INTERVAL)
          OVER (PARTITION BY instrument_id, subsystem, component ORDER BY time) AS replacement_id
      FROM
        drops
    )

  SELECT
    g.instrument_id,
    g.subsystem,
    g.component AS strip,
    MIN(g.time) AS replaced_at,
    MAX(g.prev_value) AS max_erosion_time
  FROM
    grouped g
    JOIN strips s
      USING (instrument_id, subsystem, component)
  GROUP BY
    g.instrument_id,
    g.subsystem,
    g.component,
    g.replacement_id,
    s.apertures
  HAVING
    COUNT(DISTINCT g.param_id) >= s.apertures / 2.0
  ORDER BY
    instrument_id,
    subsystem,
    strip,
    replaced_at
