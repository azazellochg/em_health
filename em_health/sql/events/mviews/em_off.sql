/* Create a materialized view with "TEM/xT server off" periods.
   Depends on em_off_daily CAGG
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS events.em_off AS
SELECT
    instrument_id,
    start_time,
    end_time
FROM events.em_off_daily,
    state_periods(agg, 1) -- boolean = 1 where value = 0
ORDER BY instrument_id, start_time
