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
        self.execute_file(self.get_path("create_metric_tables.sql", folder="db_performance"))
        logger.info("Created pganalyze tables")

    def create_metric_collectors(self) -> None:
        """ Create functions to collect statistics. """
        self.execute_file(self.get_path("create_metric_funcs.sql", folder="db_performance"))
        logger.info("Created pganalyze procedures")

    def schedule_metric_jobs(self) -> None:
        """ Schedule functions as TimescaleDB jobs. """
        jobs = [
            "SELECT add_job('pganalyze.parse_logs', schedule_interval=>'1 minutes'::interval);",
            "SELECT add_job('pganalyze.get_stat_statements', schedule_interval=>'1 minutes'::interval);",
            "SELECT add_job('pganalyze.get_db_stats', schedule_interval=>'10 minutes'::interval);",
            "SELECT add_job('pganalyze.get_table_stats', schedule_interval=>'10 minutes'::interval);",
            "SELECT add_job('pganalyze.get_index_stats', schedule_interval=>'10 minutes'::interval);",
        ]
        for j in jobs:
            self.run_query(query=j)
        logger.info("Scheduled pganalyze jobs")


def main(dbname, action):
    if action == "create-perf-stats":
        with DatabaseAnalyzer(dbname) as db:
            db.run_query("DROP SCHEMA IF EXISTS pganalyze CASCADE;")
            db.create_metric_tables()
            db.create_metric_collectors()

        with DatabaseAnalyzer(dbname, user="pganalyze", password="pganalyze") as db:
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
