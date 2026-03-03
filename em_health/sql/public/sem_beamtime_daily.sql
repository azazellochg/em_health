/* Create a materialized view of SEM states: e-beam unblank, i-beam unblank, cryocycle, xT off
   First, for the AL systems we need to merge overlapping cryocycles of the AL and column. Then union results with non-AL systems
   Note: if a system is warm / cryocycling and any beam is used, the total usage may be > 100%
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS sem_beamtime_daily AS
WITH cryocycles_al_intervals AS (
    SELECT
        instrument_id,
        day,
        start_time,
        end_time
    FROM sem_cryocycle_al_daily,
        state_periods(agg, 1) -- boolean = 1 where value = CryoCycle
),

     -- attach previous end_time to each interval. This lets you detect whether intervals overlap or are disjoint.
     cryocycles_al_ordered AS (
         SELECT
             instrument_id,
             day,
             start_time,
             end_time,
             LAG(end_time) OVER (
                 PARTITION BY instrument_id, day
                 ORDER BY start_time, end_time
                 ) AS prev_end
         FROM cryocycles_al_intervals
     ),

     cryocycles_al_grouped AS (
         SELECT
             instrument_id,
             day,
             start_time,
             end_time,
             SUM(
             CASE
                 WHEN prev_end IS NULL OR start_time > prev_end -- assign group 1 for each overlapping interval
                     THEN 1 ELSE 0
                 END
                ) OVER (
                 PARTITION BY instrument_id, day
                 ORDER BY start_time, end_time
                 ) AS grp
         FROM cryocycles_al_ordered
     ),

     -- collapse each group into single interval
     cryocycles_al_merged AS (
         SELECT
             instrument_id,
             day,
             MIN(start_time) AS start_time,
             MAX(end_time) AS end_time
         FROM cryocycles_al_grouped
         GROUP BY instrument_id, day, grp
     ),

     cryocycles AS (
         SELECT
             instrument_id,
             day,
             'cryocycle' AS state,
             SUM(end_time - start_time) AS total_duration
         FROM cryocycles_al_merged
         GROUP BY instrument_id, day, state

         UNION ALL

         SELECT
             instrument_id,
             day,
             'cryocycle' AS state,
             duration_in(agg, 1) AS total_duration -- boolean = 1 where value > 0
         FROM sem_cryocycle_noal_daily
     ),

     ebeam AS (
         SELECT instrument_id,
                day,
                'ebeam' AS state,
                toolkit_experimental.duration_in(agg, 0) AS total_duration -- state where value = 0 (unblanked)
         FROM sem_beam_daily
     ),

     ibeam AS (
         SELECT instrument_id,
                day,
                'ibeam' AS state,
                toolkit_experimental.duration_in(agg, 0) AS total_duration -- state where value = 0 (unblanked)
         FROM fib_beam_daily
     ),

     chamber AS (
         SELECT instrument_id,
                day,
                'chamber' AS state,
                toolkit_experimental.duration_in(agg, 1) AS total_duration -- boolean = 1 where value != Pumped
         FROM sem_chamber_state_daily
     ),

     sem_off AS (
         SELECT instrument_id,
                day,
                'off' AS state,
                duration_in(agg, 1) AS total_duration -- boolean = 1 where value = 0 (off)
         FROM em_off_daily
     ),

     all_states AS (
         SELECT *
         FROM cryocycles
         UNION ALL
         SELECT *
         FROM sem_off
         UNION ALL
         SELECT *
         FROM ibeam
         UNION ALL
         SELECT *
         FROM ebeam
         UNION ALL
         SELECT *
         FROM chamber
     )

SELECT
    instrument_id, day, state,
    EXTRACT('EPOCH' FROM total_duration) AS seconds
FROM all_states
ORDER BY instrument_id, day, state
