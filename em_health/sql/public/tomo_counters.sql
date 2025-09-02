/* Create a materialized view of Tomo session counters:
image counter and end state value. Sessions with 0 images are removed.
Image counter for tomo resets to "1" multiple times over the session course.
We need to sum all peaks before reset and the last peak value.

The TS counter resets to 0 multiple times over the session course.
We need to sum all peaks before reset.
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS tomo_counters AS
WITH image_counter_param AS (
    SELECT instrument_id, param_id AS image_counter_param_id
    FROM parameters
    WHERE param_name = 'TemImageCount' AND subsystem = 'Tomography'
),

     ts_counter_param AS (
         SELECT instrument_id, param_id AS ts_counter_param_id
         FROM parameters
         WHERE param_name = 'TiltSeriesNumberOfPositions' AND subsystem = 'Tomography'
     )

SELECT
    seg.instrument_id,
    seg.start_time,
    seg.end_time,
    seg.end_state_value,
    img_agg.total_image_counter,
    ts_agg.total_ts_counter
FROM tomo_sessions seg
         JOIN image_counter_param ic ON ic.instrument_id = seg.instrument_id
         LEFT JOIN ts_counter_param ts ON ts.instrument_id = seg.instrument_id
         JOIN LATERAL (
    WITH seg_data AS (
        SELECT
            d.time,
            d.value_num,
            LAG(d.value_num) OVER (ORDER BY d.time) AS prev_value,
            LEAD(d.value_num) OVER (ORDER BY d.time) AS next_value
        FROM data d
        WHERE d.instrument_id = seg.instrument_id
          AND d.param_id = ic.image_counter_param_id
          AND d.time >= seg.start_time
          AND d.time < seg.end_time
    ),
         reset_peaks AS (
             SELECT prev_value AS peak
             FROM seg_data
             WHERE value_num = 1 AND prev_value IS NOT NULL
         ),
         final_peak AS (
             SELECT value_num AS peak
             FROM seg_data
             WHERE next_value IS NULL
         )
    SELECT
        COALESCE(SUM(rp.peak), 0) + COALESCE(MAX(fp.peak), 0) AS total_image_counter
    FROM reset_peaks rp
             FULL OUTER JOIN final_peak fp ON TRUE
    ) img_agg ON TRUE

         JOIN LATERAL (
    WITH seg_data AS (
        SELECT
            d.time,
            d.value_num,
            LAG(d.value_num) OVER (ORDER BY d.time) AS prev_value
        FROM data d
        WHERE d.instrument_id = seg.instrument_id
          AND d.param_id = ts.ts_counter_param_id
          AND d.time >= seg.start_time
          AND d.time < seg.end_time
    ),
         reset_peaks AS (
             SELECT prev_value AS peak
             FROM seg_data
             WHERE value_num = 0 AND prev_value IS NOT NULL
         )
    SELECT
        COALESCE(SUM(peak), 0) AS total_ts_counter
    FROM reset_peaks
    ) ts_agg ON TRUE

WHERE img_agg.total_image_counter > 0
ORDER BY seg.instrument_id, seg.start_time
