/* Create a materialized view of EPU sessions.
   For each session, find sessionID, start and end time.
   Session IDs are assigned at session creation but not always reset to 0 at the stop.
   We assign the start time if the ID changes to any non-zero value.
   It appears that the sessionIDs are not unique so we can have duplicated IDs
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS epu_sessions AS
WITH cagg AS (
    SELECT
        d.instrument_id,
        state_agg(d.time, d.value_num::bigint) AS agg
    FROM data d
             JOIN parameters p USING (instrument_id, param_id)
    WHERE p.param_name = 'EpuSessionID'
      AND p.subsystem = 'EPU'
    GROUP BY d.instrument_id
),

     sessions AS (
         SELECT
             instrument_id,
             state AS session_id,
             start_time,
             end_time
         FROM cagg,
             state_int_timeline(agg)
         WHERE state <> 0
     )

SELECT
    instrument_id,
    session_id,
    start_time,
    end_time
FROM sessions
WHERE (end_time-start_time) > '0 seconds'
ORDER BY instrument_id, start_time
