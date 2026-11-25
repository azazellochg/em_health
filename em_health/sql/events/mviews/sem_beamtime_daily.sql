/* Create a materialized view of SEM states per day: e-beam unblank, i-beam unblank, FLM running,
   chamber not pumped, cryocycle, xT off.
   Depends on sem_cryocycles and em_off (off and down periods, both counted as off) and the state caggs.
   Cryo cycles of the autoloader and the column overlap: they are merged per instrument.
   States are logged only when they change: the state at the start of a day comes from the previous
   day with data. A day without any point of a state cagg has no row for that state.
   Note: if a system is warm / cryocycling and any beam is used, the total usage may be > 100%
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS events.sem_beamtime_daily AS
  WITH
    periods AS (
      SELECT instrument_id, 'cryocycle' AS state, start_time, end_time
      FROM events.sem_cryocycles

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

    -- collapse overlapping periods into one
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

    -- split periods into days
    period_days AS (
      SELECT
        p.instrument_id,
        d.day,
        p.state,
        SUM(LEAST(p.end_time, d.day + INTERVAL '1 day') - GREATEST(p.start_time, d.day)) AS total_duration
      FROM
        periods_merged p
        CROSS JOIN LATERAL generate_series(time_bucket('1 day', p.start_time), p.end_time, INTERVAL '1 day') AS d(day)
      WHERE
        d.day < p.end_time
      GROUP BY
        p.instrument_id,
        d.day,
        p.state
    ),

    -- the last day ends now
    ebeam AS (
      SELECT
        instrument_id,
        day,
        'ebeam' AS state,
        interpolated_duration_in(
          agg, 0, day, LEAST(INTERVAL '1 day', now() - day),
          LAG(agg) OVER (PARTITION BY instrument_id ORDER BY day)
        ) AS total_duration -- state where value = 0 (unblanked)
      FROM
        events.sem_beam_daily
    ),

    ibeam AS (
      SELECT
        instrument_id,
        day,
        'ibeam' AS state,
        interpolated_duration_in(
          agg, 0, day, LEAST(INTERVAL '1 day', now() - day),
          LAG(agg) OVER (PARTITION BY instrument_id ORDER BY day)
        ) AS total_duration -- state where value = 0 (unblanked)
      FROM
        events.fib_beam_daily
    ),

    flm AS (
      SELECT
        instrument_id,
        day,
        'flm' AS state,
        interpolated_duration_in(
          agg, 1, day, LEAST(INTERVAL '1 day', now() - day),
          LAG(agg) OVER (PARTITION BY instrument_id ORDER BY day)
        ) AS total_duration -- boolean = 1 where value = Running
      FROM
        events.flm_beam_daily
    ),

    chamber AS (
      SELECT
        instrument_id,
        day,
        'chamber' AS state,
        interpolated_duration_in(
          agg, 1, day, LEAST(INTERVAL '1 day', now() - day),
          LAG(agg) OVER (PARTITION BY instrument_id ORDER BY day)
        ) AS total_duration -- boolean = 1 where value != Pumped
      FROM
        events.sem_chamber_state_daily
    ),

    all_states AS (
      SELECT * FROM period_days
      UNION ALL
      SELECT * FROM ebeam
      UNION ALL
      SELECT * FROM ibeam
      UNION ALL
      SELECT * FROM flm
      UNION ALL
      SELECT * FROM chamber
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
