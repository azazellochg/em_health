/* Create a materialized view of EPU sessions.
   For each session, find sessionID, start and end time.
   Session IDs are assigned at session creation but not always reset to 0 at the stop.
   We assign the start time if the ID changes to any non-zero value.
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
