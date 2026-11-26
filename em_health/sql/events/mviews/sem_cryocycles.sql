/* Create a materialized view of SEM cryo cycles: Autoloader and Column of autoloader (Arctis) systems.
   Other systems have no cryo cycle.
   A cycle is a CryoCycle period of the AutoloaderTemperatureState / ColumnTemperatureState enum
   (Cryo > CryoCycle > Undefined > Cooling > Cryo): it lasts until the state changes. The state is logged
   only when it changes (a few hundred points), so the raw data is read directly.
   A cycle is complete when the next state is logged. A cycle without one is still going on and ends now.
   Cycles shorter than 1 h are aborted starts and are left out: cryo cycles last a few hours.
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS events.sem_cryocycles AS
  WITH
    state_params AS (
      SELECT
        p.instrument_id,
        p.param_id,
        REPLACE(p.param_name, 'TemperatureState', '') AS param_name,
        e.value AS cryocycle_value
      FROM
        events.parameters p
        JOIN events.enum_values e
          ON e.enum_id = p.enum_id AND e.member_name = 'CryoCycle'
      WHERE
        p.param_name LIKE '%TemperatureState'
        AND p.subsystem LIKE '%TemperatureControl'
    ),

    -- LATERAL per parameter: TimescaleDB skips compressed batches of other parameters, OFFSET 0 keeps it a LATERAL
    points AS (
      SELECT
        sp.instrument_id,
        sp.param_id,
        sp.param_name,
        d.time,
        d.value_num = sp.cryocycle_value AS is_cycle,
        LAG(d.value_num = sp.cryocycle_value) OVER (PARTITION BY sp.instrument_id, sp.param_id ORDER BY d.time)
          AS prev_is_cycle
      FROM
        state_params sp
        CROSS JOIN LATERAL (
          SELECT
            x.time,
            x.value_num
          FROM
            events.data x
          WHERE
            x.instrument_id = sp.instrument_id
            AND x.param_id = sp.param_id
          OFFSET 0
        ) d
    ),

    -- changes in and out of CryoCycle: the same state can be logged again
    changes AS (
      SELECT
        instrument_id,
        param_name,
        time,
        is_cycle,
        LEAD(time) OVER (PARTITION BY instrument_id, param_id ORDER BY time) AS next_change
      FROM
        points
      WHERE
        is_cycle IS DISTINCT FROM prev_is_cycle
    )

  SELECT
    instrument_id,
    param_name,
    time AS start_time,
    COALESCE(next_change, now()) AS end_time,
    next_change IS NOT NULL AS is_complete
  FROM
    changes
  WHERE
    is_cycle
    AND COALESCE(next_change, now()) - time >= '1 hour'::INTERVAL
  ORDER BY
    instrument_id,
    param_name,
    start_time
