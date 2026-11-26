/* Create a materialized view of Tomo acquisition runs:
   for each input session, find the total time spent in Running (Acquiring) state and whether it was Terminated.
   We filter out sessions which did not run.
   FYI the old 5.x tomo does not have Terminated state.
   Depends on tomo_sessions view
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS events.tomo_runs AS
  WITH
    state_param AS (
      SELECT
        instrument_id,
        param_id,
        enum_id
      FROM
        events.parameters
      WHERE
        param_name IN ('Tomo5TiltSeriesState', 'TiltSeries')
        AND subsystem = 'Tomography'
    ),
    state_enum AS (
      SELECT
        p.instrument_id,
        p.param_id,
        MAX(e.value) FILTER (WHERE e.member_name IN ('Running', 'Acquiring')) AS running_value,
        COALESCE(MAX(e.value) FILTER (WHERE e.member_name = 'Terminated'), -1) AS terminated_value
      FROM
        state_param p
        JOIN events.enum_values e
          ON e.enum_id = p.enum_id
      WHERE
        e.member_name IN ('Running', 'Acquiring', 'Terminated')
      GROUP BY
        p.instrument_id,
        p.param_id
    ),
    -- each state lasts until the next state point. Reading the state periods instead of only the points inside
    -- the session window carries the state at the session start in and lets the last state last until the session
    -- end: a session closed while Acquiring logs no Completed, its last batch had no running time before.
    -- LATERAL per parameter: TimescaleDB skips compressed batches of other parameters, OFFSET 0 keeps it a LATERAL.
    states AS (
      SELECT
        se.instrument_id,
        d.time AS start_time,
        LEAD(d.time) OVER (PARTITION BY se.instrument_id, se.param_id ORDER BY d.time) AS end_time,
        d.value_num = se.running_value AS is_running,
        d.value_num = se.terminated_value AS is_terminated
      FROM
        state_enum se
        CROSS JOIN LATERAL (
          SELECT
            x.time,
            x.value_num
          FROM
            events.data x
          WHERE
            x.instrument_id = se.instrument_id
            AND x.param_id = se.param_id
          OFFSET 0
        ) d
    ),
    -- state periods clipped to the session window, the last period of the data lasts until the session end
    runs AS (
      SELECT
        seg.instrument_id,
        seg.session_id,
        seg.start_time,
        seg.end_time - seg.start_time AS total_duration,
        COALESCE(SUM(LEAST(st.end_time, seg.end_time) - GREATEST(st.start_time, seg.start_time))
                   FILTER (WHERE st.is_running), '0 second'::INTERVAL) AS running_duration,
        -- only a Terminated logged in the session, not one carried in from the previous session
        COALESCE(BOOL_OR(st.is_terminated AND st.start_time >= seg.start_time), FALSE) AS has_terminated
      FROM
        events.tomo_sessions seg
        JOIN states st
          ON st.instrument_id = seg.instrument_id
          AND st.start_time < seg.end_time
          AND (st.end_time > seg.start_time OR st.end_time IS NULL)
      GROUP BY
        seg.instrument_id,
        seg.session_id,
        seg.start_time,
        seg.end_time
      ORDER BY
        seg.instrument_id,
        seg.start_time
    )
  SELECT *
  FROM
    runs
  WHERE
    running_duration > '0 second'::INTERVAL
