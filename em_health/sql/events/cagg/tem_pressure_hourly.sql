-- Create a CAGG of IGPs pressure/current: IGPcl, IGPco, IGPf/IGP3, IGPa, IGPb and HT SF6 pressure
CREATE MATERIALIZED VIEW events.tem_pressure_hourly WITH (timescaledb.continuous) AS
SELECT
    d.instrument_id,
    p.param_name,
    time_bucket('1 hour', d.time) AS hour,
    approx_percentile(0.5, percentile_agg(d.value_num)) AS mean_value,
    percentile_agg(d.value_num) AS percentile_hourly
FROM
    events.data d
        JOIN events.parameters p USING (param_id, instrument_id)
WHERE (p.component LIKE 'IGP%' AND (p.param_name LIKE 'IGP%Pressure' OR p.param_name LIKE 'IGP%Current'))
    OR (p.component = 'HTTank' AND p.param_name = 'HTTankSF6Pressure')
GROUP BY d.instrument_id, p.param_name, hour
WITH NO DATA