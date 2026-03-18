-- Create a CAGG of autoloader counters
CREATE MATERIALIZED VIEW load_counters_daily WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 day', d.time) AS day,
        d.instrument_id,
        max(CASE
            WHEN p.param_name IN ('LoadCartridgeCounter', 'SampleTransferCount') THEN d.value_num
        END) - min(CASE
            WHEN p.param_name IN ('LoadCartridgeCounter', 'SampleTransferCount') THEN d.value_num
        END) AS daily_cartridge_count,
        max(CASE
            WHEN p.param_name = 'LoadCassetteCounter' THEN d.value_num
        END) - min(CASE
            WHEN p.param_name = 'LoadCassetteCounter' THEN d.value_num
        END) AS daily_cassette_count
    FROM
        data d
        JOIN parameters p USING (param_id, instrument_id)
    WHERE p.param_name IN ('LoadCartridgeCounter', 'LoadCassetteCounter', 'SampleTransferCount')
    GROUP BY day, d.instrument_id
 WITH NO DATA
