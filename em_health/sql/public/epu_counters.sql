/* Create a materialized view of EPU session counters:
   completed and skipped images. Sessions with 0 images are removed.
   Counter does not necessarily start from 0.
   Depends on epu_sessions view
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS epu_counters AS
WITH image_counter_param AS (
    SELECT instrument_id, param_id AS image_counter_param_id
    FROM parameters
    WHERE param_name = 'CompletedExposuresCount' AND subsystem = 'EPU'
),

     skip_counter_param AS (
         SELECT instrument_id, param_id AS skip_counter_param_id
         FROM parameters
         WHERE param_name = 'SkippedExposuresCount' AND subsystem = 'EPU'
     )

SELECT
    seg.instrument_id,
    seg.session_id,
    seg.start_time,
    agg.total_image_counter::integer,
    agg.skip_image_counter::integer
FROM epu_sessions seg
         JOIN image_counter_param ic ON ic.instrument_id = seg.instrument_id
         JOIN skip_counter_param sc ON sc.instrument_id = seg.instrument_id
         JOIN LATERAL (
    SELECT
        (MAX(CASE WHEN d.param_id = ic.image_counter_param_id THEN d.value_num END)
            - MIN(CASE WHEN d.param_id = ic.image_counter_param_id THEN d.value_num END)
            ) AS total_image_counter,
        COALESCE(MAX(CASE WHEN d.param_id = sc.skip_counter_param_id THEN d.value_num END), 0) AS skip_image_counter
    FROM data d
    WHERE d.instrument_id = seg.instrument_id
      AND d.param_id IN (ic.image_counter_param_id, sc.skip_counter_param_id)
      AND d.time >= seg.start_time
      AND d.time < seg.end_time
    ) agg ON TRUE
WHERE agg.total_image_counter > 0
ORDER BY seg.instrument_id, seg.session_id