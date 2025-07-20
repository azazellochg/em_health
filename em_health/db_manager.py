# **************************************************************************
# *
# * Authors:     Grigory Sharov (gsharov@mrc-lmb.cam.ac.uk) [1]
# *
# * [1] MRC Laboratory of Molecular Biology (MRC-LMB)
# *
# * This program is free software; you can redistribute it and/or modify
# * it under the terms of the GNU General Public License as published by
# * the Free Software Foundation; either version 3 of the License, or
# * (at your option) any later version.
# *
# * This program is distributed in the hope that it will be useful,
# * but WITHOUT ANY WARRANTY; without even the implied warranty of
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# * GNU General Public License for more details.
# *
# * You should have received a copy of the GNU General Public License
# * along with this program; if not, write to the Free Software
# * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
# * 02111-1307  USA
# *
# *  All comments concerning this program package may be sent to the
# *  e-mail address 'gsharov@mrc-lmb.cam.ac.uk'
# *
# **************************************************************************
import argparse
import os
from typing import Literal, Callable

from em_health.db_client import DatabaseClient
from em_health.utils.logs import logger


class DatabaseManager(DatabaseClient):
    """ Manager class to operate on existing db.
    Creating materialized views for dashboards.
    Example usage:
        with DatabaseManager(dbname) as db:
            ...
    """
    def run_query(
            self,
            sql: str,
            mode: Literal["fetchone", "fetchmany", "fetchall", "commit", None] = "commit"
    ):
        """ Execute an SQL query and optionally return the results. """
        logger.debug(f"Executing query:\n{sql}")
        self.cur.execute(sql)

        if mode == "fetchone":
            return self.cur.fetchone()
        elif mode == "fetchmany":
            return self.cur.fetchmany()
        elif mode == "fetchall":
            return self.cur.fetchall()
        elif mode == "commit":
            self.conn.commit()
            return None
        else:
            return None

    def execute_file(self, fn) -> None:
        """ Execute an SQL file. """
        if not os.path.exists(fn):
            raise FileNotFoundError(fn)

        with open(fn, 'r') as f:
            sql = f.read()
            self.cur.execute(sql)

        self.conn.commit()

    def drop_mview(self, name: str, is_cagg: bool = False) -> None:
        """ Delete a materialized view. """
        self.run_query(f"DROP MATERIALIZED VIEW IF EXISTS {name} CASCADE;")
        if not is_cagg:
            # for standard mat. views we need to manually remove the job
            self.run_query(f"""
                SELECT delete_job(job_id)
                FROM timescaledb_information.jobs
                WHERE proc_name = 'refresh_{name}';
            """)
            self.run_query(f"DROP PROCEDURE IF EXISTS {name};")
        logger.info("Dropped materialized view %s", name)

    def schedule_mview_refresh(self, name: str, period: str = '1d') -> None:
        """ Schedule a materialized view refresh. """
        self.run_query(sql=f"""
            CREATE OR REPLACE PROCEDURE public.refresh_{name}(
                job_id int,
                config jsonb
            )
            LANGUAGE SQL
            AS $$
              REFRESH MATERIALIZED VIEW {name};
            $$;
        """)
        self.run_query(sql=f"SELECT add_job('refresh_{name}', '{period}');")
        logger.info("Scheduled refresh for %s every %s", name, period)

    def schedule_cagg_refresh(self, name: str) -> None:
        """ Schedule a cont. Aggregate refresh. """
        self.run_query(sql=f"""
            SELECT add_continuous_aggregate_policy('{name}',
            start_offset => INTERVAL '7 days',
            end_offset => INTERVAL '6 hours',
            schedule_interval => INTERVAL '12 hours');
        """)
        logger.info("Scheduled continuous aggregate refresh for %s", name)

    def force_refresh_cagg(self, name: str) -> None:
        """ Force a cont. aggregate refresh.
        The WITH NO DATA option allows the continuous aggregate to be created
        instantly, so you don't have to wait for the data to be aggregated.
        Data begins to populate only when the policy begins to run. This means
        that only data newer than the start_offset time begins to populate the
        continuous aggregate. If you have historical data that is older than
        the start_offset interval, you need to manually refresh the history
        up to the current start_offset to allow real-time queries to run efficiently.
        """
        self.conn.autocommit = True
        self.run_query(f"CALL refresh_continuous_aggregate('{name}', NULL, localtimestamp - INTERVAL '1 week');")
        self.conn.autocommit = False
        logger.info("Forced continuous aggregate refresh for %s", name)

    def create_mview(self, viewname: str) -> None:
        """ Create a new materialized view or a continuous aggregate. """
        func: Callable[[str], None] = {
            "tem_off": self.create_mview_tem_off,
            "vacuum_state_daily": self.create_mview_vacuum,

            "epu_sessions": self.create_mview_acq_sessions,
            "tomo_sessions": self.create_mview_acq_sessions,

            "epu_acquisition_daily": self.create_mview_acq_duration,
            "tomo_acquisition_daily": self.create_mview_acq_duration,
            "epu_counters": self.create_mview_epu_counters,
            "tomo_counters": self.create_mview_tomo_counters,

            "load_counters_daily": self.create_mview_autoloader_counters,
            "data_counters_daily": self.create_mview_acquired_data,
            "image_counters_daily": self.create_mview_acquired_images
        }.get(viewname)

        if func is None:
            raise Exception(f"Unknown view: {viewname}")

        func(viewname)
        logger.info("Created materialized view %s", viewname)

    ########## Mat views and aggregates #######################################
    def create_mview_vacuum(self, name: str) -> None:
        """ Create a materialized view of vacuum states. """
        self.run_query(f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {name} AS
            WITH vacuum_param AS (
              SELECT instrument_id, param_id, enum_id
              FROM parameters
              WHERE param_name = 'VacuumState'
            ),
            
            -- map enum values to open/closed/cryocycle/unknown states
            enum_states AS (
              SELECT
                e.instrument_id,
                e.value AS enum_value_num,
                CASE
                  WHEN e.name IN (
                    'ColumnConditioning', 'Column Conditioning', 'TMPmOnColumn',
                    'CryoCycle', 'Cryo Cycle', 'CryoCycle_Time', 'CryoCycle_Delay'
                  ) THEN 'cryocycle'
                  WHEN e.name IN (
                    'All Vacuum [Closed]', 'AllVacuumColumnValvesClosed', 'AllVacuum_LinersClosed'
                  ) THEN 'closed'
                  WHEN e.name IN (
                    'All Vacuum [Opened]', 'AllVacuumColumnValvesOpened', 'AllVacuum_LinersOpened'
                  ) THEN 'open'
                  ELSE 'unknown'
                END AS state
              FROM enumerations e
              WHERE (e.instrument_id, e.enum_id) IN (
                SELECT instrument_id, enum_id FROM vacuum_param
              )
            ),
            
            -- filter all vacuum states to get durations of 3 states above
            vacuum_events AS (
              SELECT
                d.instrument_id,
                d.time AS start_time,
                LEAD(d.time) OVER (PARTITION BY d.instrument_id ORDER BY d.time) AS end_time,
                es.state
              FROM data d
              JOIN enum_states es
                ON d.value_num = es.enum_value_num AND d.instrument_id = es.instrument_id
              WHERE (d.instrument_id, d.param_id) IN (
                SELECT instrument_id, param_id FROM vacuum_param
              )
              AND es.state IN ('cryocycle', 'closed', 'open')
            ),
            
            -- truncate rows to remove tem off periods
            cleaned_vacuum AS (
              SELECT
                ve.instrument_id,
                ve.start_time,
                ve.end_time,
                ve.state
              FROM vacuum_events ve
              LEFT JOIN tem_off o
                ON ve.instrument_id = o.instrument_id
                AND ve.start_time < o.end_time
                AND ve.end_time > o.start_time
              WHERE ve.end_time IS NOT NULL AND (o.start_time IS NULL OR ve.end_time <= o.start_time OR ve.start_time >= o.end_time)
            ),
            
            -- join tem off periods back
            all_states AS (
              SELECT instrument_id, start_time, end_time, state FROM cleaned_vacuum
              UNION ALL
              SELECT instrument_id, start_time, end_time, 'off' AS state FROM tem_off
            ),
            
            -- map intervals onto days
            split_intervals AS (
              SELECT
                instrument_id,
                state,
                gs::date AS day,
                GREATEST(start_time, gs) AS interval_start,
                LEAST(end_time, gs + interval '1 day') AS interval_end
              FROM all_states,
              LATERAL generate_series(
                date_trunc('day', start_time),
                date_trunc('day', end_time),
                interval '1 day'
              ) AS gs
              WHERE start_time < end_time
            )
            
            SELECT
              instrument_id,
              state,
              day,
              SUM(EXTRACT(EPOCH FROM (interval_end - interval_start))) AS seconds
            FROM split_intervals
            GROUP BY instrument_id, state, day
            ORDER BY instrument_id, day, state;
        """)

    def create_mview_tem_off(self, name: str) -> None:
        """ Create a materialized view with "TEM server off" periods.
        Normally, the server value is stored every 2 minutes. If the server goes off,
        the next value will be "1" only when it's up again. So, there are no consecutive zeros.
        """
        self.run_query(f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {name} AS
            WITH server_param AS (
              SELECT instrument_id, param_id
              FROM parameters
              WHERE component = 'Server'
                AND param_name = 'Value'
            ),
            
            server_events AS (
              SELECT
                d.instrument_id,
                d.time,
                d.value_num,
                LEAD(d.time) OVER (PARTITION BY d.instrument_id ORDER BY d.time) AS next_time
              FROM data d
              JOIN server_param sp
                ON d.instrument_id = sp.instrument_id AND d.param_id = sp.param_id
            )
            
            SELECT
              instrument_id,
              time AS start_time,
              next_time AS end_time 
            FROM server_events
            WHERE value_num = 0 AND next_time IS NOT NULL;
        """)

    def create_mview_acq_sessions(self, name: str) -> None:
        """ Create a materialized view of EPU/Tomo running states.
        This is the reference view for duration and speed views.
        Outputs start and end time for every EPU/Tomo session.
        """
        if name.startswith("epu"):
            subsystem = "EPU"
            params = "'AutomatedAcquisitionState'"
            states = "'Running'"
        else:
            subsystem = "Tomography"
            params = "'Tomo5TiltSeriesState', 'TiltSeries'"
            states = "'Acquiring', 'Running'"

        self.run_query(sql=f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {name} AS
            -- 1. Get param_id and enum_id for state
            WITH state_param AS (
              SELECT instrument_id, param_id, enum_id
              FROM parameters
              WHERE param_name IN ({params})
                AND subsystem = '{subsystem}'
            ),

            -- 2. Filter enums by both enum_id and instrument_id
            running_enum AS (
              SELECT
                p.instrument_id,
                p.param_id,
                e.value AS running_value
              FROM state_param p
              JOIN enumerations e ON e.enum_id = p.enum_id AND p.instrument_id = e.instrument_id
              WHERE e.name IN ({states})
            ),

            -- 3. Tag raw data with is_running flag
            state_data AS (
              SELECT
                d.instrument_id,
                d.time,
                d.value_num AS acquisition_state,
                CASE
                  WHEN d.value_num = r.running_value THEN 1 ELSE 0
                END AS is_running
              FROM data d
              JOIN running_enum r
                ON d.instrument_id = r.instrument_id AND d.param_id = r.param_id
            ),
            
            -- 4. Detect state transitions
            transitions AS (
                SELECT
                    instrument_id,
                    time,
                    is_running,
                    LAG(is_running) OVER (PARTITION BY instrument_id ORDER BY time) AS prev_running
                FROM state_data
            ),
            
            -- 5. Extract starts and ends
            start_events AS (
                SELECT instrument_id, time AS start_time
                FROM transitions
                WHERE is_running = 1 AND (prev_running IS NULL OR prev_running = 0)
            ),
            end_events AS (
                SELECT instrument_id, time AS end_time
                FROM transitions
                WHERE is_running = 0 AND prev_running = 1
            ),
            
            -- 6. Pair up each start with the next end
            paired_segments AS (
                SELECT
                    s.instrument_id,
                    s.start_time,
                    MIN(e.end_time) AS end_time
                FROM start_events s
                JOIN end_events e
                  ON e.instrument_id = s.instrument_id AND e.end_time > s.start_time
                GROUP BY s.instrument_id, s.start_time
            )
            
            SELECT
              p.instrument_id,
              p.start_time,
              p.end_time,
              sd.acquisition_state AS end_state_value
            FROM paired_segments p
            LEFT JOIN state_data sd
              ON p.instrument_id = sd.instrument_id AND p.end_time = sd.time
            WHERE p.end_time - p.start_time >= interval '1 second';
        """)

    def create_mview_acq_duration(self, name: str) -> None:
        """ Create a materialized view of EPU/Tomo running duration. """
        state_table = "epu_sessions" if name.startswith("epu") else "tomo_sessions"

        self.run_query(sql=f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {name} AS
            -- Break segments into daily chunks
            WITH segment_days AS (
              SELECT
                s.instrument_id,
                gs.day,
                s.start_time,
                s.end_time
              FROM {state_table} s,
              LATERAL generate_series(
                date_trunc('day', s.start_time),
                date_trunc('day', s.end_time),
                interval '1 day'
              ) AS gs(day)
            ),
            
            -- Compute overlap of each segment with its intersecting day
            running_per_day AS (
              SELECT
                instrument_id,
                day,
                GREATEST(start_time, day) AS seg_start,
                LEAST(end_time, day + interval '1 day') AS seg_end
              FROM segment_days
            )
            
            -- Sum durations per instrument per day
            SELECT
              instrument_id,
              day AS time,
              SUM(EXTRACT(EPOCH FROM (seg_end - seg_start))) AS running_duration
            FROM running_per_day
            GROUP BY instrument_id, day
        """)

    def create_mview_epu_counters(self, name: str) -> None:
        """ Create a materialized view of EPU session counters:
        image counter and end state value. Sessions with 0 images are removed.
        Counter does not necessarily start from 0.
        """
        self.run_query(sql=f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {name} AS
            WITH image_counter_param AS (
              SELECT instrument_id, param_id AS image_counter_param_id
              FROM parameters
              WHERE param_name = 'CompletedExposuresCount' AND subsystem = 'EPU'
            )
            SELECT
              seg.instrument_id,
              seg.start_time,
              seg.end_time,
              seg.end_state_value,
              agg.total_image_counter
            FROM epu_sessions seg
            JOIN image_counter_param ic ON ic.instrument_id = seg.instrument_id
            JOIN LATERAL (
              SELECT
                (MAX(d.value_num) - MIN(d.value_num)) AS total_image_counter
              FROM data d
              WHERE d.instrument_id = seg.instrument_id
                AND d.param_id = ic.image_counter_param_id
                AND d.time >= seg.start_time AND d.time < seg.end_time
            ) agg ON TRUE
            WHERE agg.total_image_counter > 0
            ORDER BY seg.instrument_id, seg.start_time;
        """)

    def create_mview_tomo_counters(self, name: str) -> None:
        """ Create a materialized view of Tomo session counters:
        image counter and end state value. Sessions with 0 images are removed.
        Image counter for tomo resets to 1 multiple times over the session course.
        We need to sum all peaks before reset and the last peak value.
        """
        self.run_query(sql=f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {name} AS
            WITH image_counter_param AS (
                SELECT instrument_id, param_id AS image_counter_param_id
                FROM parameters
                WHERE param_name = 'TemImageCount' AND subsystem = 'Tomography'
            )
            SELECT
              seg.instrument_id,
              seg.start_time,
              seg.end_time,
              seg.end_state_value,
              agg.total_image_counter
            FROM tomo_sessions seg
            JOIN image_counter_param ic ON ic.instrument_id = seg.instrument_id
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
            ) agg ON TRUE
            WHERE agg.total_image_counter > 0
            ORDER BY seg.instrument_id, seg.start_time;
        """)

    def create_mview_autoloader_counters(self, name: str) -> None:
        """ Create a materialized view of autoloader counters. """
        self.run_query(sql=f"""
            CREATE MATERIALIZED VIEW {name}
            WITH (timescaledb.continuous)
            AS
            SELECT
              time_bucket('1 day', d.time) AS day,
              d.instrument_id,
              MAX(CASE WHEN p.param_name = 'LoadCartridgeCounter' THEN d.value_num ELSE NULL END)
                - MIN(CASE WHEN p.param_name = 'LoadCartridgeCounter' THEN d.value_num ELSE NULL END) AS daily_cartridge_count,
              MAX(CASE WHEN p.param_name = 'LoadCassetteCounter' THEN d.value_num ELSE NULL END)
                - MIN(CASE WHEN p.param_name = 'LoadCassetteCounter' THEN d.value_num ELSE NULL END) AS daily_cassette_count
            FROM data d
            JOIN parameters p
              ON d.param_id = p.param_id AND d.instrument_id = p.instrument_id
            WHERE p.param_name IN ('LoadCartridgeCounter', 'LoadCassetteCounter')
            GROUP BY day, d.instrument_id
            WITH NO DATA;
        """)

    def create_mview_acquired_data(self, name: str) -> None:
        """ Create a materialized view of acquired data counter
        (Tb per day). Only Falcon cameras have such a counter.
        """
        self.run_query(sql=f"""
            CREATE MATERIALIZED VIEW {name}
            WITH (timescaledb.continuous)
            AS
            SELECT
              time_bucket('1 day', d.time) AS day,
              d.instrument_id,
              p.param_name,
              MAX(d.value_num) - MIN(d.value_num) AS daily_terabytes
            FROM data d
            JOIN parameters p
              ON d.param_id = p.param_id AND d.instrument_id = p.instrument_id
            WHERE p.param_name IN ('NumberOffloadedTerabytes', 'BM-Falcon-NumberOffloadedTB')
            GROUP BY day, d.instrument_id, p.param_name
            WITH NO DATA;
        """)

    def create_mview_acquired_images(self, name: str) -> None:
        """ Create a materialized view of acquired images counter.
        Here we count AcquisitionJobs, BM-Falcon-NumberOfAcquisitionJobs
         and AcquisitionNumber (for Gatan cameras)
        """
        self.run_query(sql=f"""
            CREATE MATERIALIZED VIEW {name}
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
            WITH NO DATA;
        """)


def main():
    msg = """
    Manage the TimescaleDB database.
        db_manager [-d DBNAME] [-m]
    """
    parser = argparse.ArgumentParser(usage=msg)
    parser.add_argument("-d", "--db", dest="db", default="tem", help="Database name (default: tem)")
    parser.add_argument("-m", "--mview", action='store_true', help="Create materialized views")

    args = parser.parse_args()
    dbname = args.db
    make_mview = args.mview

    with DatabaseManager(dbname) as db:
        if make_mview:
            mviews: dict[str, bool] = {
                # name: is_cagg
                "tem_off": False,
                "vacuum_state_daily": False,

                "epu_sessions": False,
                "tomo_sessions": False,

                "epu_acquisition_daily": False,
                "tomo_acquisition_daily": False,

                "epu_counters": False,
                "tomo_counters": False,

                "load_counters_daily": True,
                "data_counters_daily": True,
                "image_counters_daily": True,
            }

            for mview, is_cagg in mviews.items():
                db.drop_mview(mview)
                db.create_mview(mview)
                if is_cagg:
                    db.schedule_cagg_refresh(mview)
                    # Refresh only to parse historical data prior to the start_date of the schedule
                    db.force_refresh_cagg(mview)
                else:
                    db.schedule_mview_refresh(mview, '1d')
                db.run_query(f"GRANT SELECT ON public.{mview} TO grafana;")
        else:
            print("No options specified")


if __name__ == '__main__':
    main()
