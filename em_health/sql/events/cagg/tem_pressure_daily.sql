-- Create a hierarchical CAGG of IGPs pressure/current: IGPcl, IGPco, IGPf/IGP3, IGPa, IGPb and HT SF6 pressure
CREATE MATERIALIZED VIEW events.tem_pressure_daily WITH (timescaledb.continuous) AS
SELECT
    instrument_id,
    param_name,
    time_bucket('1 day', hour) AS day,
    approx_percentile(0.5, rollup(percentile_hourly)) as mean_value,
    rollup(percentile_hourly) as percentile_daily
FROM events.tem_pressure_hourly
GROUP BY instrument_id, param_name, day
WITH NO DATA