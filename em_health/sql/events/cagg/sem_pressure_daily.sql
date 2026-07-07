-- Create a hierarchical CAGG of of IGPs (1,2,3,4) pressure/current and QLG pressure
CREATE MATERIALIZED VIEW events.sem_pressure_daily WITH (timescaledb.continuous) AS
SELECT
    instrument_id,
    param_name,
    time_bucket('1 day', hour) AS day,
    approx_percentile(0.5, rollup(percentile_hourly)) as mean_value,
    rollup(percentile_hourly) as percentile_daily
FROM events.sem_pressure_hourly
GROUP BY instrument_id, param_name, day
WITH NO DATA