-- Create a hierarchical CAGG of IGPs (1,2,3,4) pressure/current and QLG pressure
-- Hourly, daily and monthly tiers are kept forever, Grafana picks the tier by time range
CREATE MATERIALIZED VIEW events.sem_pressure_monthly WITH (timescaledb.continuous) AS
  SELECT
    instrument_id,
    param_name,
    time_bucket('1 month', day) AS month,
    approx_percentile(0.5, rollup(percentile_daily)) AS mean_value,
    rollup(percentile_daily) AS percentile_monthly
  FROM
    events.sem_pressure_daily
  GROUP BY
    instrument_id,
    param_name,
    month
WITH NO DATA
