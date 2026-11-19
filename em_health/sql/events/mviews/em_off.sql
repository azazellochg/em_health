/* Create a materialized view with "TEM/xT server off" periods.
   Depends on em_off_daily CAGG
   When the server is off, nothing is logged until it is up again, so an outage that crosses midnight
   has no points on the next day: the state at the start of a day comes from the previous day with data.
   Such an outage comes as two periods (until midnight, from midnight), merged here.
   An outage that is still going on at the end of the data ends at the end of that day (or now).
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS events.em_off AS
  WITH days AS (
    SELECT
      instrument_id,
      day,
      agg,
      LAG(agg) OVER (PARTITION BY instrument_id ORDER BY day) AS prev_agg
    FROM
      events.em_off_daily
  ),

  periods AS (
    SELECT
      d.instrument_id,
      d.day,
      s.start_time,
      s.end_time
    FROM
      days d
      CROSS JOIN LATERAL interpolated_state_periods(d.agg, 1, d.day, '1 day', d.prev_agg) s -- 1 where value = 0
  ),

  -- a period that starts at midnight continues the outage of the previous day
  outages AS (
    SELECT
      *,
      COUNT(*) FILTER (WHERE start_time > day) OVER (PARTITION BY instrument_id ORDER BY start_time) AS outage_id
    FROM
      periods
  )

  SELECT
    instrument_id,
    MIN(start_time) AS start_time,
    LEAST(MAX(end_time), now()) AS end_time
  FROM
    outages
  GROUP BY
    instrument_id,
    outage_id
  ORDER BY
    instrument_id,
    start_time
