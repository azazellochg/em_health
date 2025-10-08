/* Create a materialized view of Tomo Running state duration per day
   Depends on tomo_state_daily view
 */
CREATE MATERIALIZED VIEW tomo_running_daily AS
WITH state_param AS (
    SELECT instrument_id, param_id, enum_id
    FROM parameters
    WHERE param_name IN ('Tomo5TiltSeriesState', 'TiltSeries')
      AND subsystem = 'Tomography'
),

     state_enum AS (
         SELECT
             p.instrument_id,
             p.param_id,
             MAX(e.value) FILTER (WHERE e.member_name IN ('Running', 'Acquiring')) AS running_value
         FROM state_param p
                  JOIN enum_values e ON e.enum_id = p.enum_id
         WHERE e.member_name IN ('Running', 'Acquiring')
         GROUP BY p.instrument_id, p.param_id
     )
SELECT
    instrument_id,
    time,
    toolkit_experimental.interpolated_duration_in(
            agg,
            se.running_value,
            time,
            '1 day',
            LAG(agg) OVER (PARTITION BY instrument_id ORDER BY time)
    ) AS running_duration
FROM tomo_state_daily
         JOIN state_enum se USING (instrument_id)
ORDER BY instrument_id, time
