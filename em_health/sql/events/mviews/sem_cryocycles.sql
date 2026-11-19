/* Create a materialized view of SEM cryo cycle periods.
   Depends on sem_cryocycle_al_daily (autoloader systems: Autoloader and Column TemperatureState = CryoCycle)
   and sem_cryocycle_noal_daily (non-autoloader systems: cryo shield temperature > 0, param_name CryoShield).
   The state is logged only when it changes, so a cycle that crosses midnight has no points on the next day:
   the state at the start of a day comes from the previous day with data. Such a cycle comes as two periods
   (until midnight, from midnight), merged here.
   A cycle that is still going on at the end of the data ends at the end of that day (or now).
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS events.sem_cryocycles AS
  WITH days AS (
    SELECT
      instrument_id,
      param_name,
      day,
      agg,
      LAG(agg) OVER (PARTITION BY instrument_id, param_name ORDER BY day) AS prev_agg
    FROM
      events.sem_cryocycle_al_daily

    UNION ALL

    SELECT
      instrument_id,
      'CryoShield' AS param_name,
      day,
      agg,
      LAG(agg) OVER (PARTITION BY instrument_id ORDER BY day) AS prev_agg
    FROM
      events.sem_cryocycle_noal_daily
  ),

  periods AS (
    SELECT
      d.instrument_id,
      d.param_name,
      d.day,
      s.start_time,
      s.end_time
    FROM
      days d
      CROSS JOIN LATERAL interpolated_state_periods(d.agg, 1, d.day, '1 day', d.prev_agg) s -- 1 = cryo cycle
  ),

  -- a period that starts at midnight continues the cycle of the previous day
  cycles AS (
    SELECT
      *,
      COUNT(*) FILTER (WHERE start_time > day)
        OVER (PARTITION BY instrument_id, param_name ORDER BY start_time) AS cycle_id
    FROM
      periods
  )

  SELECT
    instrument_id,
    param_name,
    MIN(start_time) AS start_time,
    LEAST(MAX(end_time), now()) AS end_time
  FROM
    cycles
  GROUP BY
    instrument_id,
    param_name,
    cycle_id
  ORDER BY
    instrument_id,
    param_name,
    start_time
