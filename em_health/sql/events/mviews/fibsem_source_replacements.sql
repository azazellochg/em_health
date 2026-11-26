/* Create a materialized view of FIB/SEM source replacements: one row per replacement of the FIB (LMIS/PFIB) or
   FEG source. The source age counter (s: SourceAgeParameter or OperatingTimeFIBParameter on the FIB,
   OperateTimeFEGParameter on the FEG) is reset when the source is replaced. A replacement is a drop from a non-zero
   value to 0 or below half: after a restart the counter can come back a little lower (a restored value), which is
   not a replacement. age_at_replacement (s) is the last value before the drop: the age of the replaced source.
   A cagg can't do this: it does not see the value before the first point of a day.
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS events.fibsem_source_replacements AS
  WITH
    counter_params AS (
      SELECT
        instrument_id,
        param_id,
        component
      FROM
        events.parameters
      WHERE
        (component = 'FIB' AND param_name IN ('SourceAgeParameter', 'OperatingTimeFIBParameter'))
        OR (component = 'FEG' AND param_name = 'OperateTimeFEGParameter')
    ),

    -- LATERAL per parameter: TimescaleDB skips compressed batches of other parameters, OFFSET 0 keeps it a LATERAL
    points AS (
      SELECT
        cp.instrument_id,
        cp.component,
        d.time,
        d.value_num,
        LAG(d.value_num) OVER (PARTITION BY cp.instrument_id, cp.param_id ORDER BY d.time) AS prev_value
      FROM
        counter_params cp
        CROSS JOIN LATERAL (
          SELECT
            x.time,
            x.value_num
          FROM
            events.data x
          WHERE
            x.instrument_id = cp.instrument_id
            AND x.param_id = cp.param_id
          OFFSET 0
        ) d
    )

  SELECT
    instrument_id,
    component,
    time AS replaced_at,
    prev_value AS age_at_replacement
  FROM
    points
  WHERE
    prev_value > 0
    AND value_num < prev_value / 2
  ORDER BY
    instrument_id,
    component,
    replaced_at
