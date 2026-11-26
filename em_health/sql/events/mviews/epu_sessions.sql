/* Create a materialized view of EPU sessions.
   For each session, find sessionID, start and end time.
   Session IDs are assigned at session creation but not always reset to 0 at the stop.
   We assign the start time if the ID changes to any non-zero value.
   The session ends when its acquisition state last changed to a final state (Stopped, Completed, Terminated),
   not when the next ID is logged: without a reset to 0 the ID stays until the next session, often hours or days.
   It appears that the sessionIDs are not unique so we can have duplicated IDs
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS events.epu_sessions AS
  WITH
    params AS (
      SELECT
        instrument_id,
        param_id
      FROM
        events.parameters
      WHERE
        param_name = 'EpuSessionID'
        AND subsystem = 'EPU'
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

    state_param AS (
      SELECT
        p.instrument_id,
        p.param_id,
        e.value AS running_value
      FROM
        events.parameters p
        JOIN events.enum_values e
          ON e.enum_id = p.enum_id AND e.member_name = 'Running'
      WHERE
        p.param_name = 'AutomatedAcquisitionState'
        AND p.subsystem = 'EPU'
    ),

    -- acquisition state changes: EPU logs the same state again, e.g. Completed hours later when the session is closed
    state_changes AS (
      SELECT
        instrument_id,
        time,
        is_running
      FROM
        (
          SELECT
            sp.instrument_id,
            d.time,
            d.value_num = sp.running_value AS is_running,
            d.value_num IS DISTINCT FROM LAG(d.value_num) OVER (PARTITION BY sp.instrument_id ORDER BY d.time)
              AS is_change
          FROM
            state_param sp
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
        ) c
      WHERE
        is_change
    ),

    -- the session ends at its last state change unless it is still Running (EPU crashed or the session goes on).
    -- The last second is skipped: on some instruments the next session's Running is logged a few ms before its ID.
    -- The end is exclusive (time < end_time in the views using the sessions): 1 ms after the last state change
    -- keeps it in the session, e.g. Terminated
    sessions_end AS (
      SELECT DISTINCT ON (s.instrument_id, s.start_time)
        s.instrument_id,
        s.session_id,
        s.start_time,
        CASE WHEN c.is_running IS FALSE THEN c.time + '1 millisecond'::INTERVAL ELSE s.end_time END AS end_time
      FROM
        sessions s
        LEFT JOIN state_changes c
          ON c.instrument_id = s.instrument_id
          AND c.time >= s.start_time
          AND c.time < s.end_time - '1 second'::INTERVAL
      ORDER BY
        s.instrument_id,
        s.start_time,
        c.time DESC
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
