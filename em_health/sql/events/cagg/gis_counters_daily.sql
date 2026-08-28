-- Create a CAGG of GIS usage time counters
CREATE MATERIALIZED VIEW events.gis_counters_daily WITH (timescaledb.continuous) AS
  SELECT
    d.instrument_id,
    time_bucket('1 day', d.time) AS day,
    counter_agg(d.time, d.value_num) AS agg,
    p.component AS gis_port
  FROM
    events.data d
    JOIN events.parameters p
      USING (param_id, instrument_id)
  WHERE
    p.subsystem = 'GISes'
    AND param_name LIKE 'GIS%ValveOpenTimeParameter'
  GROUP BY
    d.instrument_id,
    day,
    gis_port
WITH NO DATA