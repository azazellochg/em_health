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

"""
Contains code from https://github.com/pganalyze/collector project

Copyright (c) 2016, pganalyze Team <team@pganalyze.com>
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

* Neither the name of pganalyze nor the names of its contributors may be used
to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
"""

import os

from em_health.db_manager import DatabaseManager
from em_health.utils.tools import logger


class DatabaseAnalyzer(DatabaseManager):
    """ This class contains methods to collect and
    analyze database performance. Metrics are based on
    https://github.com/pganalyze/collector
    """
    def create_metric_tables(self) -> None:
        """ Create tables to store metrics data. """
        self.execute_file(self.get_path("create_tables.sql", folder="pganalyze"),
                          {
                              "var_pgsnaps_chunk_size": os.getenv("TBL_SNAPS_CHUNK_SIZE", "4 weeks"),
                              "var_pgstats_chunk_size": os.getenv("TBL_STATS_CHUNK_SIZE", "1 week"),
                              "var_pgstats_compression": os.getenv("TBL_STATS_COMPRESSION", "7 days"),
                              "var_pgstats_retention": os.getenv("TBL_STATS_RETENTION", "6 months")
                          })
        logger.info("Created pganalyze tables")

    def create_metric_collectors(self) -> None:
        """ Create functions to collect statistics. """
        self.execute_file(self.get_path("create_functions.sql", folder="pganalyze"))
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
        self.execute_file(self.get_path("create_jobs.sql", folder="pganalyze"))
        logger.info("Scheduled pganalyze jobs")

    # FIXME: not used at the moment
    def create_stats_cagg(self):
        """ Create cagg for pganalyze.stat_statements."""
        mview = "pganalyze.stat_statements_cagg"
        self.drop_mview(mview, is_cagg=True)
        self.create_mview(mview)
        self.force_refresh_cagg(mview)
        self.schedule_cagg_refresh(mview,
                                   start_offset="10 minutes",
                                   end_offset="0 minutes",
                                   interval="5 minutes")


def main(dbname, action, force=False):
    if action == "create-perf-stats":
        with DatabaseAnalyzer(dbname) as db:
            if force:  # erase all data
                db.run_query("DROP SCHEMA IF EXISTS pganalyze CASCADE;")
                db.create_metric_tables()
            else:
                # keep the tables, only update jobs
                db.cleanup_jobs()

            db.create_metric_collectors()

        with DatabaseAnalyzer(dbname, username="pganalyze", password="POSTGRES_PGANALYZE_PASSWORD") as db:
            db.schedule_metric_jobs()

    elif action in ["run-query", "explain-query"]:
        custom_query = """
            -- paste your query below
            select * from pg_stat_statements limit 1
        """

        if action == "explain-query":
            custom_query = "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) " + custom_query

        with DatabaseAnalyzer(dbname) as db:
            result = db.run_query(custom_query, mode="fetchall")
            formatted_output = "\n".join(row[0] for row in result)
            print(formatted_output)
