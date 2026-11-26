/* Create a materialized view of EPU running time per day (UTC days):
   the Running state periods of the sessions, split by day.
   Each state lasts until the next state point and is clipped to its session, like epu_runs:
   the days add up to the epu_runs running time.
   Depends on epu_sessions view
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS events.epu_running_daily AS
  WITH
    state_enum AS (
      SELECT
        p.instrument_id,
        p.param_id,
        MAX(e.value) AS running_value
      FROM
        events.parameters p
        JOIN events.enum_values e
          ON e.enum_id = p.enum_id
      WHERE
        p.param_name = 'AutomatedAcquisitionState'
        AND p.subsystem = 'EPU'
        AND e.member_name IN ('Running')
      GROUP BY
        p.instrument_id,
        p.param_id
    ),
    -- LATERAL per parameter: TimescaleDB skips compressed batches of other parameters, OFFSET 0 keeps it a LATERAL
    states AS (
      SELECT
        se.instrument_id,
        d.time AS start_time,
        LEAD(d.time) OVER (PARTITION BY se.instrument_id, se.param_id ORDER BY d.time) AS end_time,
        d.value_num = se.running_value AS is_running
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
    -- running periods clipped to the session, the last period of the data lasts until the session end
    running AS (
      SELECT
        seg.instrument_id,
        GREATEST(st.start_time, seg.start_time) AS start_time,
        LEAST(st.end_time, seg.end_time) AS end_time
      FROM
        events.epu_sessions seg
        JOIN states st
          ON st.instrument_id = seg.instrument_id
          AND st.start_time < seg.end_time
          AND (st.end_time > seg.start_time OR st.end_time IS NULL)
      WHERE
        st.is_running
    )
  SELECT
    r.instrument_id,
    gs AS time,
    SUM(LEAST(r.end_time, gs + '1 day'::INTERVAL) - GREATEST(r.start_time, gs)) AS running_duration
  FROM
    running r
    CROSS JOIN LATERAL GENERATE_SERIES(time_bucket('1 day', r.start_time), r.end_time, '1 day'::INTERVAL) gs
  WHERE
    -- a period ending at midnight has no time on the next day
    gs < r.end_time
  GROUP BY
    r.instrument_id,
    gs
  ORDER BY
    r.instrument_id,
    time
