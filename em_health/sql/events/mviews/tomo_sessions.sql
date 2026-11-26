/* Create a materialized view of Tomo sessions.
For each session, find sessionID, start and end time.
NOTES:
- Session IDs are assigned at session creation but not always reset to 0 at the stop.
   Also, a single session can be stopped/completed/terminated multiple times
   during its time course.
- It appears that the sessionIDs are not unique so we can have duplicated IDs
- The session ends at its last activity, not when the next ID (or 0) is logged, often hours later:
   the last tilt series state change or the last non-zero image, tilt series or search map counter point.
   A session closed while Acquiring logs no Completed, its acquisition ended with the last counter point.
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS events.tomo_sessions AS
  WITH
    params AS (
      SELECT
        instrument_id,
        param_id
      FROM
        events.parameters
      WHERE
        param_name = 'SessionId'
        AND subsystem = 'Tomography'
    ),

    -- LATERAL with a constant param_id per loop lets compressed batches be skipped by
    -- their param_id min/max, a plain JOIN decompresses all instrument data.
    -- OFFSET 0 keeps the planner from flattening the subquery back into a join.
    -- The LATERAL only fetches rows, aggregation stays outside: works for any aggregate.
    cagg AS (
      SELECT
        p.instrument_id,
        state_agg(d.time, d.value_num::BIGINT) AS agg
      FROM
        params p
        CROSS JOIN LATERAL (
          SELECT
            d.time,
            d.value_num
          FROM
            events.data d
          WHERE
            d.instrument_id = p.instrument_id
            AND d.param_id = p.param_id
          OFFSET 0
        ) d
      GROUP BY
        p.instrument_id
    ),

    sessions AS (
      SELECT instrument_id, state AS session_id, start_time, end_time
      FROM cagg, state_int_timeline(agg)
      WHERE state <> 0
    ),

    -- tilt series state changes: the same state can be logged again.
    -- MATERIALIZED: computed once, not again for each session in the LATERAL below
    state_changes AS MATERIALIZED (
      SELECT
        instrument_id,
        time
      FROM
        (
          SELECT
            p.instrument_id,
            d.time,
            d.value_num IS DISTINCT FROM LAG(d.value_num) OVER (PARTITION BY p.instrument_id ORDER BY d.time)
              AS is_change
          FROM
            events.parameters p
            CROSS JOIN LATERAL (
              SELECT
                x.time,
                x.value_num
              FROM
                events.data x
              WHERE
                x.instrument_id = p.instrument_id
                AND x.param_id = p.param_id
              OFFSET 0
            ) d
          WHERE
            p.param_name = 'Tomo5TiltSeriesState'
            AND p.subsystem = 'Tomography'
        ) c
      WHERE
        is_change
    ),

    counter_params AS (
      SELECT
        instrument_id,
        param_id
      FROM
        events.parameters
      WHERE
        param_name IN ('TemImageCount', 'TiltSeriesNumberOfPositions', 'SearchMapsPerJob')
        AND subsystem = 'Tomography'
    ),

    -- last activity in the session. The last second is skipped: values logged again with the next session ID
    last_activity AS (
      SELECT
        s.instrument_id,
        s.start_time,
        MAX(a.time) AS time
      FROM
        sessions s
        CROSS JOIN LATERAL (
          SELECT
            c.time
          FROM
            state_changes c
          WHERE
            c.instrument_id = s.instrument_id
            AND c.time >= s.start_time
            AND c.time < s.end_time - '1 second'::INTERVAL
          UNION ALL
          SELECT
            MAX(x.time)
          FROM
            counter_params cp
            CROSS JOIN LATERAL (
              SELECT
                y.time
              FROM
                events.data y
              WHERE
                y.instrument_id = cp.instrument_id
                AND y.param_id = cp.param_id
                AND y.time >= s.start_time
                AND y.time < s.end_time - '1 second'::INTERVAL
                AND y.value_num <> 0
              ORDER BY
                y.time DESC
              LIMIT 1
            ) x
          WHERE
            cp.instrument_id = s.instrument_id
        ) a
      GROUP BY
        s.instrument_id,
        s.start_time
    ),

    -- sessions without any activity keep their end. The end is exclusive (time < end_time in the views using the
    -- sessions): 1 ms after the last activity keeps its point in the session
    sessions_end AS (
      SELECT
        s.instrument_id,
        s.session_id,
        s.start_time,
        COALESCE(la.time + '1 millisecond'::INTERVAL, s.end_time) AS end_time
      FROM
        sessions s
        LEFT JOIN last_activity la
          USING (instrument_id, start_time)
    )

  SELECT
    instrument_id,
    session_id,
    start_time,
    end_time
  FROM
    sessions_end
  WHERE
    (end_time - start_time) > '0 seconds'
  ORDER BY
    instrument_id,
    start_time
