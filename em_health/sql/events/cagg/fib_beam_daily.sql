/* Create a CAGG of FIB beam blank states
   Here we ignore the BeamIsOnFIBParameter since it's very rare to have beam unblanked and beam off.
 */
CREATE MATERIALIZED VIEW events.fib_beam_daily WITH (timescaledb.continuous) AS
SELECT
    d.instrument_id,
    time_bucket('1 day', d.time) AS day,
    toolkit_experimental.compact_state_agg(d.time, d.value_num::int) AS agg
FROM
    events.data d
        JOIN events.parameters p USING (instrument_id, param_id)
WHERE
    p.param_name IN ('IsBlankedFIBParameter', 'IsBlankedParameter')
  AND p.component = 'FIB'
GROUP BY d.instrument_id, day
WITH NO DATA