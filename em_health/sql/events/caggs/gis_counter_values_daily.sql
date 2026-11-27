/* Create a CAGG of GIS valve open time counter values per day (used by the view events.gis_counters_daily).
   Zeros are ignored: the counter sometimes logs a single 0 and then returns to its value.
   Some instruments store the counter in hours: values are converted to seconds.
*/
CREATE MATERIALIZED VIEW events.gis_counter_values_daily WITH (timescaledb.continuous) AS
  SELECT
    d.instrument_id,
    time_bucket('1 day', d.time) AS day,
    p.component AS gis_port,
    first(d.value_num, d.time) * MAX(CASE p.storage_unit WHEN 'h' THEN 3600 ELSE 1 END) AS first_value,
    last(d.value_num, d.time) * MAX(CASE p.storage_unit WHEN 'h' THEN 3600 ELSE 1 END) AS last_value,
    MIN(d.value_num) * MAX(CASE p.storage_unit WHEN 'h' THEN 3600 ELSE 1 END) AS min_value,
    MAX(d.value_num) * MAX(CASE p.storage_unit WHEN 'h' THEN 3600 ELSE 1 END) AS max_value
  FROM
    events.data d
    JOIN events.parameters p
      USING (param_id, instrument_id)
  WHERE
    p.subsystem = 'GISes'
    AND p.param_name LIKE 'GIS%ValveOpenTimeParameter'
    AND d.value_num > 0
  GROUP BY
    d.instrument_id,
    day,
    gis_port
WITH NO DATA
