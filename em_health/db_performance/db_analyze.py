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

import os
from psycopg.rows import dict_row

from em_health.db_manager import DatabaseManager
from em_health.utils.logs import logger


class DatabaseAnalyzer(DatabaseManager):
    """ This class contains methods to collect and
    analyze database performance. Metrics are based on
    https://github.com/pganalyze/collector
    """
    def create_metric_tables(self) -> None:
        """ Create tables to store metrics data. """
        self.execute_file(self.get_path("create_metric_tables.sql", folder="db_performance"),
                          {
                              "TBL_STATEMENTS_INTERVAL": os.getenv("TBL_STATEMENTS_INTERVAL", "6 hours"),
                              "TBL_STATEMENTS_COMPRESSION": os.getenv("TBL_STATEMENTS_COMPRESSION", "7 days"),
                              "TBL_STATS_RETENTION": os.getenv("TBL_STATS_RETENTION", "3 months")
                          })
        logger.info("Created pganalyze tables")

    def create_metric_collectors(self) -> None:
        """ Create functions to collect statistics. """
        self.execute_file(self.get_path("create_metric_funcs.sql", folder="db_performance"),
                          {
                              "DBNAME": self.db_name,
                              "POSTGRES_PGANALYZE_PASSWORD": os.getenv("POSTGRES_PGANALYZE_PASSWORD"),
                              "TBL_STATS_RETENTION": os.getenv("TBL_STATS_RETENTION", "3 months")
                          })
        logger.info("Created pganalyze procedures")

    def cleanup_jobs(self) -> None:
        """ Delete existing jobs. """
        jobs = self.run_query(
            "SELECT job_id FROM timescaledb_information.jobs WHERE proc_schema = 'pganalyze'",
            mode="fetchall")

        if jobs:
            self.cur.executemany("SELECT delete_job(%s)", jobs)
            self.conn.commit()

    def schedule_metric_jobs(self) -> None:
        """ Schedule functions as TimescaleDB jobs. """
        logs_interval = os.getenv("JOB_LOGS_INTERVAL", "1 minutes")
        statements_interval = os.getenv("JOB_STATEMENTS_INTERVAL", "1 minutes")
        dbstats_interval = os.getenv("JOB_DBSTATS_INTERVAL", "10 minutes")
        tblstats_interval = os.getenv("JOB_TBLSTATS_INTERVAL", "10 minutes")
        idxstats_interval = os.getenv("JOB_IDXSTATS_INTERVAL", "10 minutes")

        jobs = [
            f"SELECT add_job('pganalyze.parse_logs', schedule_interval=>'{logs_interval}'::interval);",
            f"SELECT add_job('pganalyze.get_stat_statements', schedule_interval=>'{statements_interval}'::interval);",
            f"SELECT add_job('pganalyze.get_db_stats', schedule_interval=>'{dbstats_interval}'::interval);",
            f"SELECT add_job('pganalyze.get_table_stats', schedule_interval=>'{tblstats_interval}'::interval);",
            f"SELECT add_job('pganalyze.get_index_stats', schedule_interval=>'{idxstats_interval}'::interval);",
            "SELECT add_job('pganalyze.purge_stats', schedule_interval=>'1 day'::interval);"
        ]
        for j in jobs:
            self.run_query(query=j)
        logger.info("Scheduled pganalyze jobs")

    def create_stats_cagg(self):
        """ Create cagg for pganalyze.stat_statements."""
        mview = "stat_statements_cagg"
        self.drop_mview(mview)
        self.create_mview(mview)
        self.force_refresh_cagg(mview)
        self.schedule_cagg_refresh(mview, start_offset="3 months",
                                   end_offset="3 minutes", interval="1 minute")


def main(dbname, action, force=False):
    if action == "create-perf-stats":
        with DatabaseAnalyzer(dbname) as db:
            if force: # erase all data
                db.run_query("DROP SCHEMA IF EXISTS pganalyze CASCADE;")
                db.create_metric_tables()
            else:
                # keep the tables, only update jobs
                db.cleanup_jobs()

            db.create_metric_collectors()
            db.create_stats_cagg()

        pwd = os.getenv("POSTGRES_PGANALYZE_PASSWORD")
        with DatabaseAnalyzer(dbname, username="pganalyze", password=pwd) as db:
            db.schedule_metric_jobs()

    elif action in ["run-query", "explain-query"]:
        custom_query = """
            -- paste your query below
        """

        if action == "explain-query":
            custom_query = "EXPLAIN (ANALYZE, BUFFERS) " + custom_query

        with DatabaseAnalyzer(dbname) as db:
            result = db.run_query(custom_query, mode="fetchall", row_factory=dict_row)
            print(result)
