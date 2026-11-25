/* Create a materialized view with "TEM/xT server off" periods, built from the raw server heartbeat
   (Server Value = 1, logged every 120 s on TEMs and every 60 s on SEMs). state is:
   - off: controlled shutdown. The server logs a 0 just before it stops; the period lasts until the next 1.
     Repeated zeros belong to the same period.
   - down: the heartbeat stopped without a 0 (crash or missing data). At least 2 beats are missing
     (one missing beat is a dropped point). The period starts when the first missing beat was due and ends
     at the next point.
   An off period that is still going on at the end of the data ends now. A silence at the end of the data is
   not reported as down: newer data may not have been imported yet.
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS events.em_off AS
  WITH
    beat AS (
      SELECT make_interval(secs => CASE current_database() WHEN 'tem' THEN 120 ELSE 60 END) AS nominal
    ),

    -- data is read per parameter (LATERAL): instrument_id and param_id are then constants for the scan, so
    -- TimescaleDB skips compressed batches of other parameters (about 10x faster than a join)
    heartbeat AS (
      SELECT
        d.instrument_id,
        d.time,
        d.value_num,
        LAG(d.value_num) OVER w AS prev_value,
        LEAD(d.time) OVER w AS next_time
      FROM
        events.parameters p
        CROSS JOIN LATERAL (
          SELECT x.instrument_id, x.time, x.value_num
          FROM events.data x
          WHERE x.instrument_id = p.instrument_id
            AND x.param_id = p.param_id
          OFFSET 0
        ) d
      WHERE
        p.component = 'Server'
        AND p.param_name = 'Value'
      WINDOW w AS (PARTITION BY d.instrument_id ORDER BY d.time)
    ),

    -- a zero after a non-zero starts a new off period, repeated zeros continue it
    zeros AS (
      SELECT
        instrument_id,
        time,
        next_time,
        COUNT(*) FILTER (WHERE prev_value IS DISTINCT FROM 0)
          OVER (PARTITION BY instrument_id ORDER BY time) AS period_id
      FROM
        heartbeat
      WHERE
        value_num = 0
    )

  SELECT
    instrument_id,
    MIN(time) AS start_time,
    CASE WHEN bool_or(next_time IS NULL) THEN now() ELSE MAX(next_time) END AS end_time,
    'off' AS state
  FROM
    zeros
  GROUP BY
    instrument_id,
    period_id

  UNION ALL

  SELECT
    h.instrument_id,
    h.time + b.nominal AS start_time,
    h.next_time AS end_time,
    'down' AS state
  FROM
    heartbeat h
    CROSS JOIN beat b
  WHERE
    h.value_num = 1
    -- 2 missed beats = 3 intervals; 2.5 leaves room for jitter
    AND h.next_time - h.time > 2.5 * b.nominal

  ORDER BY
    instrument_id,
    start_time
