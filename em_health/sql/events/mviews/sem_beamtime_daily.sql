/* Create a materialized view of SEM states per day: e-beam unblank, i-beam unblank, FLM running,
   chamber vacuum actions, cryocycle, xT off.
   Depends on sem_cryocycles, em_off (off and down periods, both counted as off) and the beam caggs.
   - ebeam, ibeam, flm: periods from the daily state caggs. The state at the start of a day comes from the previous
     day with data (the beam states are logged every few seconds, every day has data).
   - chamber: vacuum actions (Pumping, Venting, Vented, PlasmaCleaning, Busy) from raw data: the chamber state is
     logged only when it changes, a cagg has no row for a day without a change. Unknown and Uninitialized (the
     software does not know the state, e.g. after a restart) and Pumped are no vacuum actions.
   - cryocycle: sem_cryocycles. Cryo cycles of the autoloader and the column overlap: they are merged per instrument.
   The off periods are cut out of all other states: during a gap in the data the last state would go on
   (e.g. a beam unblanked when the server went down), and a state during a shutdown would be counted twice.
   Note: if a system is cryocycling and any beam is used, the total usage may be > 100%
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS events.sem_beamtime_daily AS
  WITH
    chamber_params AS (
      SELECT
        instrument_id,
        param_id,
        enum_id
      FROM
        events.parameters
      WHERE
        param_name = 'Chamber.StateSC'
        AND component = 'Chamber'
    ),

    -- LATERAL per parameter: TimescaleDB skips compressed batches of other parameters, OFFSET 0 keeps it a LATERAL.
    -- A state lasts until the next point.
    chamber_points AS (
      SELECT
        cp.instrument_id,
        d.time,
        LEAD(d.time) OVER (PARTITION BY cp.instrument_id, cp.param_id ORDER BY d.time) AS next_time,
        e.member_name IN ('Pumping', 'Venting', 'Vented', 'PlasmaCleaning', 'Busy') AS is_action
      FROM
        chamber_params cp
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
        LEFT JOIN events.enum_values e
          ON e.enum_id = cp.enum_id AND e.value = d.value_num
    ),

    periods AS (
      SELECT instrument_id, 'cryocycle' AS state, start_time, end_time
      FROM events.sem_cryocycles

      UNION ALL

      SELECT instrument_id, 'chamber' AS state, time AS start_time, COALESCE(next_time, now()) AS end_time
      FROM chamber_points
      WHERE is_action

      UNION ALL

      SELECT instrument_id, 'off' AS state, start_time, end_time
      FROM events.em_off
    ),

    -- end of the previous periods, to detect overlapping periods
    periods_ordered AS (
      SELECT
        *,
        MAX(end_time) OVER (
          PARTITION BY instrument_id, state
          ORDER BY start_time, end_time
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_end
      FROM
        periods
    ),

    periods_grouped AS (
      SELECT
        *,
        COUNT(*) FILTER (WHERE prev_end IS NULL OR start_time > prev_end)
          OVER (PARTITION BY instrument_id, state ORDER BY start_time, end_time) AS grp
      FROM
        periods_ordered
    ),

    -- collapse overlapping (and adjacent) periods into one
    periods_merged AS (
      SELECT
        instrument_id,
        state,
        MIN(start_time) AS start_time,
        MAX(end_time) AS end_time
      FROM
        periods_grouped
      GROUP BY
        instrument_id,
        state,
        grp
    ),

    -- cut the off periods out of the cryocycle and chamber periods
    periods_cut AS (
      SELECT
        m.instrument_id,
        m.state,
        lower(r) AS start_time,
        upper(r) AS end_time
      FROM
        periods_merged m
        CROSS JOIN LATERAL (
          SELECT range_agg(tstzrange(o.start_time, o.end_time)) AS off_ranges
          FROM events.em_off o
          WHERE o.instrument_id = m.instrument_id AND o.start_time < m.end_time AND o.end_time > m.start_time
        ) o
        CROSS JOIN LATERAL unnest(
          tstzmultirange(tstzrange(m.start_time, m.end_time)) - COALESCE(o.off_ranges, '{}'::tstzmultirange)
        ) r
      WHERE
        m.state <> 'off'

      UNION ALL

      SELECT instrument_id, state, start_time, end_time
      FROM periods_merged
      WHERE state = 'off'
    ),

    -- split periods into days
    period_days AS (
      SELECT
        p.instrument_id,
        d.day,
        p.state,
        SUM(LEAST(p.end_time, d.day + INTERVAL '1 day') - GREATEST(p.start_time, d.day)) AS total_duration
      FROM
        periods_cut p
        CROSS JOIN LATERAL generate_series(time_bucket('1 day', p.start_time), p.end_time, INTERVAL '1 day') AS d(day)
      WHERE
        d.day < p.end_time
      GROUP BY
        p.instrument_id,
        d.day,
        p.state
    ),

    -- off periods per instrument and day, to cut them out of the beam periods of that day
    off_days AS (
      SELECT
        o.instrument_id,
        d.day,
        range_agg(tstzrange(GREATEST(o.start_time, d.day), LEAST(o.end_time, d.day + INTERVAL '1 day'))) AS off_ranges
      FROM
        events.em_off o
        CROSS JOIN LATERAL generate_series(time_bucket('1 day', o.start_time), o.end_time, INTERVAL '1 day') AS d(day)
      WHERE
        d.day < o.end_time
      GROUP BY
        o.instrument_id,
        d.day
    ),

    beam_days AS (
      SELECT instrument_id, day, 'ebeam' AS state, 0::BIGINT AS on_state, agg,  -- unblanked
             LAG(agg) OVER (PARTITION BY instrument_id ORDER BY day) AS prev_agg
      FROM events.sem_beam_daily

      UNION ALL

      SELECT instrument_id, day, 'ibeam' AS state, 0::BIGINT AS on_state, agg,  -- unblanked
             LAG(agg) OVER (PARTITION BY instrument_id ORDER BY day) AS prev_agg
      FROM events.fib_beam_daily

      UNION ALL

      SELECT instrument_id, day, 'flm' AS state, 1::BIGINT AS on_state, agg,  -- boolean = 1 where value = Running
             LAG(agg) OVER (PARTITION BY instrument_id ORDER BY day) AS prev_agg
      FROM events.flm_beam_daily
    ),

    -- the last day ends now
    beam_periods AS (
      SELECT
        b.instrument_id,
        b.day,
        b.state,
        s.start_time,
        s.end_time
      FROM
        beam_days b
        CROSS JOIN LATERAL interpolated_state_periods(
          b.agg, b.on_state, b.day, LEAST(INTERVAL '1 day', now() - b.day), b.prev_agg
        ) s
    ),

    beam_totals AS (
      SELECT
        bp.instrument_id,
        bp.day,
        bp.state,
        SUM(
          CASE
            WHEN od.off_ranges IS NULL THEN bp.end_time - bp.start_time
            ELSE (
              SELECT COALESCE(SUM(upper(r) - lower(r)), INTERVAL '0')
              FROM unnest(tstzmultirange(tstzrange(bp.start_time, bp.end_time)) - od.off_ranges) r
            )
          END
        ) AS total_duration
      FROM
        beam_periods bp
        LEFT JOIN off_days od
          USING (instrument_id, day)
      GROUP BY
        bp.instrument_id,
        bp.day,
        bp.state
    ),

    all_states AS (
      SELECT * FROM period_days
      UNION ALL
      SELECT * FROM beam_totals
    )

  SELECT
    instrument_id,
    day,
    state,
    EXTRACT('EPOCH' FROM total_duration) AS seconds
  FROM
    all_states
  ORDER BY
    instrument_id,
    day,
    state
