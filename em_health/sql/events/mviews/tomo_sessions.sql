/* Create a materialized view of Tomo sessions.
For each session, find sessionID, start and end time.
NOTES:
- Session IDs are assigned at session creation but not always reset to 0 at the stop.
   Also, a single session can be stopped/completed/terminated multiple times
   during its time course.
- It appears that the sessionIDs are not unique so we can have duplicated IDs
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS events.tomo_sessions AS
  WITH
    cagg AS (
      SELECT
        d.instrument_id,
        state_agg(d.time, d.value_num::BIGINT) AS agg
      FROM
        events.data d
        JOIN events.parameters p
          USING (instrument_id, param_id)
      WHERE
        p.param_name = 'SessionId'
        AND p.subsystem = 'Tomography'
      GROUP BY
        d.instrument_id
    ),

    sessions AS (
      SELECT instrument_id, state AS session_id, start_time, end_time
      FROM cagg, state_int_timeline(agg)
      WHERE state <> 0
    )

  SELECT
    instrument_id,
    session_id,
    start_time,
    end_time
  FROM
    sessions
  WHERE
    (end_time - start_time) > '0 seconds'
  ORDER BY
    instrument_id,
    start_time
