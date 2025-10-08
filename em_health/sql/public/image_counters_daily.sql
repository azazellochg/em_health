/* Create a CAGG of acquired images counter.
   Here we count AcquisitionJobs, BM-Falcon-NumberOfAcquisitionJobs
   and AcquisitionNumber (for Gatan cameras)
*/
CREATE MATERIALIZED VIEW image_counters_daily
            WITH (timescaledb.continuous)
AS
SELECT
    time_bucket('1 day', d.time) AS day,
    d.instrument_id,
    p.param_name,
    MAX(d.value_num) - MIN(d.value_num) AS daily_images
FROM data d
         JOIN parameters p
              ON d.param_id = p.param_id AND d.instrument_id = p.instrument_id
WHERE p.param_name IN ('AcquisitionJobs', 'BM-Falcon-NumberOfAcquisitionJobs', 'AcquisitionNumber')
GROUP BY day, d.instrument_id, p.param_name
WITH NO DATA