-- Create a materialized view of vacuum states
CREATE MATERIALIZED VIEW IF NOT EXISTS vacuum_state_daily AS
WITH vacuum_param AS (
    SELECT instrument_id, param_id, enum_id
    FROM parameters
    WHERE param_name = 'VacuumState'
),

     -- map enum values to open/closed/cryocycle/unknown states
     enum_states AS (
         SELECT
             vp.instrument_id,
             e.value AS enum_value,
             CASE
                 WHEN e.member_name IN (
                                        'ColumnConditioning', 'Column Conditioning', 'TMPmOnColumn',
                                        'CryoCycle', 'Cryo Cycle', 'CryoCycle_Time', 'CryoCycle_Delay'
                     ) THEN 'cryocycle'
                 WHEN e.member_name IN (
                                        'All Vacuum [Closed]', 'AllVacuumColumnValvesClosed', 'AllVacuum_LinersClosed'
                     ) THEN 'closed'
                 WHEN e.member_name IN (
                                        'All Vacuum [Opened]', 'AllVacuumColumnValvesOpened', 'AllVacuum_LinersOpened'
                     ) THEN 'open'
                 ELSE 'unknown'
                 END AS state
         FROM enum_values e
                  JOIN vacuum_param vp ON e.enum_id = vp.enum_id
     ),

     -- filter all vacuum states to get durations of 3 states above
     vacuum_events AS (
         SELECT
             d.instrument_id,
             d.time AS start_time,
             LEAD(d.time) OVER (PARTITION BY d.instrument_id ORDER BY d.time) AS end_time,
             es.state
         FROM data d
                  JOIN enum_states es
                       ON d.value_num = es.enum_value AND d.instrument_id = es.instrument_id
         WHERE (d.instrument_id, d.param_id) IN (
             SELECT instrument_id, param_id FROM vacuum_param
         )
           AND es.state IN ('cryocycle', 'closed', 'open')
     ),

     -- truncate rows to remove tem off periods
     cleaned_vacuum AS (
         SELECT
             ve.instrument_id,
             ve.start_time,
             ve.end_time,
             ve.state
         FROM vacuum_events ve
                  LEFT JOIN tem_off o
                            ON ve.instrument_id = o.instrument_id
                                AND ve.start_time < o.end_time
                                AND ve.end_time > o.start_time
         WHERE ve.end_time IS NOT NULL AND (o.start_time IS NULL OR ve.end_time <= o.start_time OR ve.start_time >= o.end_time)
     ),

     -- join tem off periods back
     all_states AS (
         SELECT instrument_id, start_time, end_time, state FROM cleaned_vacuum
         UNION ALL
         SELECT instrument_id, start_time, end_time, 'off' AS state FROM tem_off
     ),

     -- map intervals onto days
     split_intervals AS (
         SELECT
             instrument_id,
             state,
             gs::date AS day,
             GREATEST(start_time, gs) AS interval_start,
             LEAST(end_time, gs + interval '1 day') AS interval_end
         FROM all_states,
              LATERAL generate_series(
                      date_trunc('day', start_time),
                      date_trunc('day', end_time),
                      interval '1 day'
                      ) AS gs
         WHERE start_time < end_time
     )

SELECT
    instrument_id,
    state,
    day,
    SUM(EXTRACT(EPOCH FROM (interval_end - interval_start))) AS seconds
FROM split_intervals
GROUP BY instrument_id, state, day
ORDER BY instrument_id, day, state