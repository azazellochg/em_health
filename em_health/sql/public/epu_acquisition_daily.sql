-- Create a materialized view of EPU running duration
CREATE MATERIALIZED VIEW IF NOT EXISTS epu_acquisition_daily AS
    -- Break segments into daily chunks
WITH segment_days AS (
    SELECT
        s.instrument_id,
        gs.day,
        s.start_time,
        s.end_time
    FROM epu_sessions s,
         LATERAL generate_series(
                 date_trunc('day', s.start_time),
                 date_trunc('day', s.end_time),
                 interval '1 day'
                 ) AS gs(day)
),

     -- Compute the overlap of each segment with its intersecting day
     running_per_day AS (
         SELECT
             instrument_id,
             day,
             GREATEST(start_time, day) AS seg_start,
             LEAST(end_time, day + interval '1 day') AS seg_end
         FROM segment_days
         WHERE GREATEST(start_time, day) < LEAST(end_time, day + interval '1 day')
     )

-- Sum durations per instrument per day
SELECT
    instrument_id,
    day AS time,
    SUM(EXTRACT(EPOCH FROM (seg_end - seg_start))) AS running_duration
FROM running_per_day
GROUP BY instrument_id, day