# **************************************************************************
# *
# * Authors:     Grigory Sharov (gsharov@mrclmb.ac.uk) [1]
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
# *  e-mail address 'gsharov@mrclmb.ac.uk'
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
    def create_tables(self) -> None:
        """ Create tables to store metrics data. """
        self.execute_file(self.get_path("pganalyze/create_tables.sql"),
                          {
                              "var_pgstats_chunk_size": os.getenv("TBL_STATS_CHUNK_SIZE", "1 week"),
                              "var_pgstats_retention": os.getenv("TBL_STATS_RETENTION", "3 months")
                          })
        logger.info("Created pganalyze tables")

    def create_funcs(self) -> None:
        """ Create functions to collect statistics. """
        self.execute_file(self.get_path("pganalyze/create_functions.sql"))
        logger.info("Created pganalyze procedures")

    def delete_jobs(self) -> None:
        """ Delete existing jobs. """
        jobs = self.run_query(
            "SELECT job_id FROM timescaledb_information.jobs WHERE proc_schema = %s",
            values=('pganalyze',),
            mode="fetchall")

        if jobs:
            self.cur.executemany("SELECT delete_job(%s)", jobs)
            self.conn.commit()

    def schedule_jobs(self) -> None:
        """ Schedule functions as TimescaleDB jobs. """
        self.execute_file(self.get_path("pganalyze/create_jobs.sql"))
        logger.info("Scheduled pganalyze jobs")


def main(dbname, action, force=False):
    if action == "pganalyze":
        with DatabaseAnalyzer(dbname, username="pganalyze", password="POSTGRES_PGANALYZE_PASSWORD") as db:
            db.delete_jobs()

            if force:  # erase all data
                db.run_query("""
                    DROP TABLE IF EXISTS pganalyze.database_stats CASCADE;
                    DROP TABLE IF EXISTS pganalyze.table_stats CASCADE;
                    DROP TABLE IF EXISTS pganalyze.index_stats CASCADE;
                    DROP TABLE IF EXISTS pganalyze.vacuum_stats CASCADE;
                    DROP TABLE IF EXISTS pganalyze.stat_snapshots CASCADE;
                    DROP TABLE IF EXISTS pganalyze.queries CASCADE;
                    DROP TABLE IF EXISTS pganalyze.stat_statements CASCADE;
                    DROP TABLE IF EXISTS pganalyze.stat_explains CASCADE;
                    DROP TABLE IF EXISTS pganalyze.sys_stats CASCADE;
                """)
                db.create_tables()

            db.create_funcs()
            db.schedule_jobs()
