/* Create a materialized view of SEM temperature state periods: cryo, rt (room temperature), transition or unknown.
   param_name tells the source:
   - Column: autoloader (Arctis) systems, ColumnTemperatureState: Cryo is cryo, Room is rt, Heating, Cooling and
     CryoCycle are transition, Undefined, Error and Service are unknown.
   - CryoStage: systems with a cryo stage thermometer (Aquilos), ExperimentalThermometerCryoStageTemperature:
     below -150 C is cryo, above +15 C is rt, in between is transition (warming up or cooling down).
     The temperature at room temperature dips below +15 C for a few seconds: a transition shorter than 5 min
     between two periods of the same state belongs to that state.
   A period lasts until the state changes, the last one until the last heartbeat of the instrument (the end of the
   imported data). Systems without either source have no rows.
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS events.sem_temperature_states AS
  WITH
    sources AS (
      SELECT
        instrument_id,
        param_id,
        'Column' AS param_name,
        enum_id
      FROM
        events.parameters
      WHERE
        param_name = 'ColumnTemperatureState'
        AND subsystem = 'ColumnTemperatureControl'

      UNION ALL

      SELECT
        instrument_id,
        param_id,
        'CryoStage' AS param_name,
        NULL AS enum_id
      FROM
        events.parameters
      WHERE
        param_name = 'ExperimentalThermometerCryoStageTemperature'
    ),

    -- LATERAL per parameter: TimescaleDB skips compressed batches of other parameters, OFFSET 0 keeps it a LATERAL
    points AS (
      SELECT
        s.instrument_id,
        s.param_id,
        s.param_name,
        d.time,
        CASE
          WHEN s.enum_id IS NOT NULL THEN
            CASE
              WHEN e.member_name = 'Cryo' THEN 'cryo'
              WHEN e.member_name = 'Room' THEN 'rt'
              WHEN e.member_name IN ('Heating', 'Cooling', 'CryoCycle') THEN 'transition'
              ELSE 'unknown'
            END
          WHEN d.value_num < -150 THEN 'cryo'
          WHEN d.value_num > 15 THEN 'rt'
          ELSE 'transition'
        END AS state
      FROM
        sources s
        CROSS JOIN LATERAL (
          SELECT
            x.time,
            x.value_num
          FROM
            events.data x
          WHERE
            x.instrument_id = s.instrument_id
            AND x.param_id = s.param_id
          OFFSET 0
        ) d
        LEFT JOIN events.enum_values e
          ON e.enum_id = s.enum_id AND e.value = d.value_num
    ),

    -- end of the imported data: the last heartbeat of the instrument
    data_end AS (
      SELECT
        p.instrument_id,
        MAX(h.time) AS time
      FROM
        events.parameters p
        CROSS JOIN LATERAL (
          SELECT
            x.time
          FROM
            events.data x
          WHERE
            x.instrument_id = p.instrument_id
            AND x.param_id = p.param_id
          ORDER BY
            x.time DESC
          LIMIT 1
        ) h
      WHERE
        p.component = 'Server'
        AND p.param_name = 'Value'
      GROUP BY
        p.instrument_id
    ),

    -- one row per state change
    changes AS (
      SELECT
        *
      FROM
        (
          SELECT
            *,
            LAG(state) OVER (PARTITION BY instrument_id, param_id ORDER BY time) AS prev_point_state
          FROM
            points
        ) p
      WHERE
        state IS DISTINCT FROM prev_point_state
    ),

    runs AS (
      SELECT
        instrument_id,
        param_id,
        param_name,
        state,
        time AS start_time,
        LEAD(time) OVER w AS end_time,
        LAG(state) OVER w AS prev_state,
        LEAD(state) OVER w AS next_state
      FROM
        changes
      WINDOW w AS (PARTITION BY instrument_id, param_id ORDER BY time)
    ),

    -- thermometer: a dip into transition shorter than 5 min between two periods of the same state
    smoothed AS (
      SELECT
        instrument_id,
        param_id,
        param_name,
        start_time,
        end_time,
        CASE
          WHEN param_name = 'CryoStage' AND state = 'transition' AND prev_state = next_state
            AND end_time - start_time < '5 minutes'::INTERVAL THEN prev_state
          ELSE state
        END AS state
      FROM
        runs
    ),

    -- merge consecutive runs of the same state
    grouped AS (
      SELECT
        *,
        COUNT(*) FILTER (WHERE state IS DISTINCT FROM prev_state)
          OVER (PARTITION BY instrument_id, param_id ORDER BY start_time) AS period_id
      FROM
        (
          SELECT
            *,
            LAG(state) OVER (PARTITION BY instrument_id, param_id ORDER BY start_time) AS prev_state
          FROM
            smoothed
        ) s
    )

  SELECT
    g.instrument_id,
    g.param_name,
    g.state,
    MIN(g.start_time) AS start_time,
    CASE WHEN BOOL_OR(g.end_time IS NULL) THEN GREATEST(COALESCE(MAX(de.time), now()), MIN(g.start_time))
         ELSE MAX(g.end_time) END AS end_time
  FROM
    grouped g
    LEFT JOIN data_end de
      USING (instrument_id)
  GROUP BY
    g.instrument_id,
    g.param_id,
    g.param_name,
    g.state,
    g.period_id
  ORDER BY
    instrument_id,
    param_name,
    start_time
