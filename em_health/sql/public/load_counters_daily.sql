-- Create a CAGG of autoloader counters
CREATE MATERIALIZED VIEW load_counters_daily
            WITH (timescaledb.continuous)
AS
SELECT
    time_bucket('1 day', d.time) AS day,
    d.instrument_id,
    MAX(CASE WHEN p.param_name = 'LoadCartridgeCounter' THEN d.value_num END)
        - MIN(CASE WHEN p.param_name = 'LoadCartridgeCounter' THEN d.value_num END) AS daily_cartridge_count,
    MAX(CASE WHEN p.param_name = 'LoadCassetteCounter' THEN d.value_num END)
        - MIN(CASE WHEN p.param_name = 'LoadCassetteCounter' THEN d.value_num END) AS daily_cassette_count
FROM data d
         JOIN parameters p
              ON d.param_id = p.param_id AND d.instrument_id = p.instrument_id
WHERE p.param_name IN ('LoadCartridgeCounter', 'LoadCassetteCounter')
GROUP BY day, d.instrument_id
WITH NO DATA