/* Create a CAGG of acquisition counter values per day (used by the view events.image_counters_daily):
   AcquisitionJobs, BM-Falcon-NumberOfAcquisitionJobs, BM-Ceta-NumberOfAcquisitionJobs and
   AcquisitionNumber (for Gatan cameras).
*/
CREATE MATERIALIZED VIEW events.image_counter_values_daily WITH (timescaledb.continuous) AS
  SELECT
    time_bucket('1 day', d.time) AS day,
    d.instrument_id,
    p.param_name,
    first(d.value_num, d.time) AS first_value,
    last(d.value_num, d.time) AS last_value,
    MIN(d.value_num) AS min_value,
    MAX(d.value_num) AS max_value
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
    AND d.value_num <> 0
  GROUP BY
    day,
    d.instrument_id,
    p.param_name
WITH NO DATA
