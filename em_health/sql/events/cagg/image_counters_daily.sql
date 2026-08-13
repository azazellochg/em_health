/* Create a CAGG of acquired images counter.
   Here we count AcquisitionJobs, BM-Falcon-NumberOfAcquisitionJobs, BM-Ceta-NumberOfAcquisitionJobs
   and AcquisitionNumber (for Gatan cameras)
*/
CREATE MATERIALIZED VIEW events.image_counters_daily WITH (timescaledb.continuous) AS
  SELECT
    time_bucket('1 day', d.time) AS day,
    d.instrument_id,
    p.param_name,
    delta(counter_agg(d.time, d.value_num))::INT AS daily_images
  FROM
    events.data d
    JOIN events.parameters p
      USING (instrument_id, param_id)
  WHERE
    p.param_name IN ('AcquisitionJobs',
                     'BM-Falcon-NumberOfAcquisitionJobs',
                     'BM-Ceta-NumberOfAcquisitionJobs',
                     'AcquisitionNumber'
      )
  GROUP BY
    day,
    d.instrument_id,
    p.param_name
WITH NO DATA
