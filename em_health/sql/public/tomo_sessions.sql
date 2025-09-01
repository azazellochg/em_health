/* Create a materialized view of Tomo running states.
This is the reference view for duration and speed views.
Outputs start and end time for every Tomo session.
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS tomo_sessions AS
    -- 1. Get param_id and enum_id for state
WITH state_param AS (
    SELECT instrument_id, param_id, enum_id
    FROM parameters
    WHERE param_name IN ('Tomo5TiltSeriesState', 'TiltSeries')
      AND subsystem = 'Tomography'
),

     -- 2. Filter enums by both enum_id and instrument_id
     state_enum AS (
         SELECT
             p.instrument_id,
             p.param_id,
             MAX(e.value) FILTER (WHERE e.member_name IN ('Running', 'Acquiring')) AS running_value,
             MAX(e.value) FILTER (WHERE e.member_name = 'Paused')  AS paused_value
         FROM state_param p
                  JOIN enum_values e ON e.enum_id = p.enum_id
         WHERE e.member_name IN ('Running', 'Acquiring', 'Paused')
         GROUP BY p.instrument_id, p.param_id
     ),

     -- 3. Tag raw data with is_running flag, remove paused states
     state_data AS (
         SELECT
             d.instrument_id,
             d.time,
             d.value_num AS acquisition_state,
             CASE
                 WHEN d.value_num = se.running_value THEN 1 ELSE 0
                 END AS is_running
         FROM data d
                  JOIN state_enum se
                       ON d.instrument_id = se.instrument_id AND d.param_id = se.param_id
         WHERE d.value_num != se.paused_value
         ORDER BY d.instrument_id, d.time
     ),

     -- 4. Detect state transitions
     transitions AS (
         SELECT
             instrument_id,
             time,
             is_running,
             LAG(is_running) OVER (PARTITION BY instrument_id ORDER BY time) AS prev_running
         FROM state_data
     ),

     -- 5. Extract starts and ends
     start_events AS (
         SELECT instrument_id, time AS start_time
         FROM transitions
         WHERE is_running = 1 AND (prev_running IS NULL OR prev_running = 0)
     ),
     end_events AS (
         SELECT instrument_id, time AS end_time
         FROM transitions
         WHERE is_running = 0 AND prev_running = 1
     ),

     -- 6. Pair up each start with the next end
     paired_segments AS (
         SELECT
             s.instrument_id,
             s.start_time,
             MIN(e.end_time) AS end_time
         FROM start_events s
                  JOIN end_events e
                       ON e.instrument_id = s.instrument_id AND e.end_time > s.start_time
         GROUP BY s.instrument_id, s.start_time
     )

SELECT
    p.instrument_id,
    p.start_time,
    p.end_time,
    sd.acquisition_state AS end_state_value
FROM paired_segments p
         LEFT JOIN state_data sd
                   ON p.instrument_id = sd.instrument_id AND p.end_time = sd.time
WHERE p.end_time - p.start_time >= interval '10 second'