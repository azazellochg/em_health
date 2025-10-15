/* Create a materialized view with "TEM server off" periods.
   Normally, the server value is stored every 2 minutes. If the server goes off,
   the next value will be "1" only when it's up again. So, there are no consecutive zeros.
   It could happen that the TEM server crashed or powered off and there was no 0 recorded.
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS tem_off AS
WITH server_param AS (
    SELECT instrument_id, param_id
    FROM parameters
    WHERE component = 'Server'
      AND param_name = 'Value'
),

     server_events AS (
         SELECT
             d.instrument_id,
             d.time,
             d.value_num,
             LEAD(d.time) OVER (PARTITION BY d.instrument_id ORDER BY d.time) AS next_time
         FROM data d
                  JOIN server_param sp
                       ON d.instrument_id = sp.instrument_id AND d.param_id = sp.param_id
     )

SELECT
    instrument_id,
    time AS start_time,
    next_time AS end_time
FROM server_events
WHERE value_num = 0 AND next_time IS NOT NULL