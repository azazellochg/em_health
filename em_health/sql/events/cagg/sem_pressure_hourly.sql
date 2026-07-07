-- Create a CAGG of IGPs (1,2,3,4) pressure/current and QLG pressure
CREATE MATERIALIZED VIEW events.sem_pressure_hourly WITH (timescaledb.continuous) AS
SELECT
    d.instrument_id,
    p.param_name,
    time_bucket('1 hour', d.time) AS hour,
    approx_percentile(0.5, percentile_agg(d.value_num)) AS mean_value,
    percentile_agg(d.value_num) AS percentile_hourly
FROM
    events.data d
        JOIN events.parameters p USING (param_id, instrument_id)
WHERE (p.component LIKE '%-Column' AND (p.param_name LIKE '%-Column.IGP%' OR p.param_name LIKE 'IGP_Parameter') AND p.display_name NOT LIKE '%Non-filtered%')
    OR (p.component = 'Quickloader' AND p.param_name = 'Quickloader.QLG')
GROUP BY d.instrument_id, p.param_name, hour
WITH NO DATA