/* Create a materialized view of EPU sessions.
   For each session, find sessionID, start and end time.
   Session IDs are assigned at session creation but not always reset to 0 at the stop.
   We assign the start time if the ID changes to any non-zero value.
   It appears that the sessionIDs are not unique so we can have duplicated IDs
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS epu_sessions AS
WITH session_param AS (
    SELECT instrument_id, param_id
    FROM parameters
    WHERE param_name = 'EpuSessionID'
      AND subsystem = 'EPU'
),
     raw AS (
         SELECT d.time, d.instrument_id, d.param_id, d.value_num
         FROM data d
                  JOIN session_param sp
                       USING (instrument_id, param_id)
     ),
     changes AS (
         SELECT
             time,
             instrument_id,
             param_id,
             value_num,
             LAG(value_num) OVER (
                 PARTITION BY instrument_id, param_id
                 ORDER BY time
                 ) AS prev_value
         FROM raw
     ),
     change_points AS (
         SELECT
             time,
             instrument_id,
             param_id,
             value_num
         FROM changes
         WHERE value_num <> prev_value
     ),
     session_boundaries AS (
         SELECT
             instrument_id,
             param_id,
             value_num AS session_id,
             time AS start_time,
             LEAD(time) OVER (
                 PARTITION BY instrument_id, param_id
                 ORDER BY time
                 ) AS end_time
         FROM change_points
     )
SELECT
    instrument_id,
    session_id::integer,
    start_time,
    end_time
FROM session_boundaries
WHERE session_id <> 0
ORDER BY instrument_id, start_time
