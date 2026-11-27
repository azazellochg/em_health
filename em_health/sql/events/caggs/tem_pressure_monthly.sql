-- Create a hierarchical CAGG of IGPs pressure/current: IGPcl, IGPco, IGPf/IGP3, IGPa, IGPb and HT SF6 pressure
-- Hourly, daily and monthly tiers are kept forever, Grafana picks the tier by time range
CREATE MATERIALIZED VIEW events.tem_pressure_monthly WITH (timescaledb.continuous) AS
  SELECT
    instrument_id,
    param_name,
    time_bucket('1 month', day) AS month,
    approx_percentile(0.5, rollup(percentile_daily)) AS mean_value,
    rollup(percentile_daily) AS percentile_monthly
  FROM
    events.tem_pressure_daily
  GROUP BY
    instrument_id,
    param_name,
    month
WITH NO DATA
