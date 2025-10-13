/* Create a materialized view of Tomo acquisition runs:
   for each input session, find the total time spent in Running and Terminated states.
   We filter out sessions which did not run.
   FYI the old 5.x tomo does not have Terminated state.
   Depends on tomo_sessions view
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS tomo_runs AS
WITH state_param AS (
    SELECT instrument_id, param_id, enum_id
    FROM parameters
    WHERE param_name IN ('Tomo5TiltSeriesState', 'TiltSeries')
      AND subsystem = 'Tomography'
),
state_enum AS (
    SELECT
        p.instrument_id,
        p.param_id,
        MAX(e.value) FILTER (WHERE e.member_name IN ('Running', 'Acquiring'))    AS running_value,
        COALESCE(MAX(e.value) FILTER (WHERE e.member_name = 'Terminated'), -1) AS terminated_value
    FROM state_param p
             JOIN enum_values e ON e.enum_id = p.enum_id
    WHERE e.member_name IN ('Running', 'Acquiring', 'Terminated')
    GROUP BY p.instrument_id, p.param_id
),
     runs AS (
         SELECT
             seg.instrument_id,
             seg.session_id,
             seg.start_time,
             (seg.end_time-seg.start_time) AS total_duration,
             SUM(CASE WHEN v.state = se.running_value THEN v.duration ELSE INTERVAL '0 second' END) AS running_duration,
             BOOL_OR(v.state = se.terminated_value) AS has_terminated
         FROM tomo_sessions seg
                  JOIN state_enum se USING (instrument_id)
                  JOIN state_param sp
                       ON se.instrument_id = sp.instrument_id
                           AND se.param_id = sp.param_id
                  JOIN LATERAL (
             SELECT state, duration
             FROM toolkit_experimental.into_int_values(
                     (
                         SELECT toolkit_experimental.compact_state_agg(d.time, d.value_num::bigint)
                         FROM data d
                         WHERE d.instrument_id = sp.instrument_id
                           AND d.param_id = sp.param_id
                           AND d.time >= seg.start_time
                           AND d.time < seg.end_time
                     )
                  )
             ) v ON TRUE
         GROUP BY seg.instrument_id, seg.session_id, seg.start_time, seg.end_time
         ORDER BY seg.instrument_id, seg.start_time)
SELECT * FROM runs
WHERE running_duration > INTERVAL '0 second'
