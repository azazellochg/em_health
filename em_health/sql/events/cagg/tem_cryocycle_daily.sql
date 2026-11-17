/* Create a CAGG of TEM cryo cycles.
   The *CryoCycleDuration counter is the number of seconds since the cycle start, so all points
   of one cycle share cycle_start = time - counter. A cycle ends at its peak (peak_value = duration):
   the last point before the counter is reset to 0 (logged at the next point) or drops (a new cycle
   started without a logged 0). Each logged 0 is a row with peak_value = 0.
   cycle_start is rounded to a second, so rows of one cycle can differ by a second, and a cycle
   that crosses midnight has a row per day: merge rows with close cycle_start values when querying.
   The duration returned is always based on the temperature algorithm
   (if the conditioning is set to "delayed", e.g. 12h, the returned value will still be only a few hours).
*/
CREATE MATERIALIZED VIEW events.tem_cryocycle_daily WITH (timescaledb.continuous) AS
  SELECT
    d.instrument_id,
    time_bucket('1 day', d.time) AS day,
    REPLACE(p.param_name, 'CryoCycleDuration', '') AS param_name,
    -- timestamptz - timestamptz is immutable, unlike EXTRACT(EPOCH FROM timestamptz)
    to_timestamp(round(EXTRACT(EPOCH FROM d.time - 'epoch'::timestamptz) - d.value_num)) AS cycle_start,
    MIN(d.time) AS first_time,
    MAX(d.time) AS last_time,
    MAX(d.value_num) AS peak_value
  FROM
    events.data d
    JOIN events.parameters p
      USING (instrument_id, param_id)
  WHERE
    p.param_name IN
    ('AutoloaderCryoCycleDuration', 'ColumnCryoCycleDuration', 'CryoCycleDurationCLS', 'CryoCycleDurationMicroscope')
  GROUP BY
    d.instrument_id,
    p.param_name,
    day,
    cycle_start
WITH NO DATA
