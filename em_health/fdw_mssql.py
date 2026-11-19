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


import os

from em_health.fdw_manager import ForeignServer
from em_health.utils.tools import logger


class MssqlFdw(ForeignServer):
    """ tds_fdw server for the microscope SQL Server DS database, with the UEC foreign tables
    read by uec.import_errors. Only the qry views are used.
    UECs are only recorded on TEMs.
    """
    prefix = "ms"

    def setup_server(self):
        """ Create a foreign data wrapper for a MSSQL database. """
        user = os.getenv("MSSQL_USER")

        self.dbm.run_query("""
            CREATE SERVER IF NOT EXISTS {name}
            FOREIGN DATA WRAPPER tds_fdw
            OPTIONS (
                servername {server},
                port '57659',
                database 'DS',
                row_estimate_method 'showplan_all'
            );

            CREATE USER MAPPING IF NOT EXISTS FOR emhealth
            SERVER {name}
            OPTIONS (username {user}, password {password});
        """, identifiers={"name": self.name}, strings={
            "server": self.server,
            "user": user,
            "password": os.getenv("MSSQL_PASSWORD")
        })

        logger.info("Setup foreign server MSSQL %s@%s:57659 database DS",
                    user, self.server)

    def create_tables(self):
        """ Create foreign tables for MSSQL in the shared fdw schema, named <server>_<table>
        (fdw.ms_<id>_error_definitions, fdw.ms_<id>_error_notifications), as uec.import_errors expects.
        Recreated every time: a foreign table holds no data, and IF NOT EXISTS would keep an old definition.
        """
        self.dbm.run_query("""
            DROP FOREIGN TABLE IF EXISTS {table};
            CREATE FOREIGN TABLE {table} (
                ErrorDefinitionID INTEGER,
                SubsystemID INTEGER,
                Subsystem TEXT,
                DeviceTypeID INTEGER,
                DeviceType TEXT,
                DeviceInstanceID INTEGER,
                DeviceInstance TEXT,
                ErrorCodeID INTEGER,
                ErrorCode TEXT
            ) SERVER {name}
            OPTIONS (schema_name 'qry', table_name 'ErrorDefinitions');
        """, {"table": self.table("error_definitions"), "name": self.name})

        # ErrorDtm is a datetimeoffset in local time. FreeTDS drops the offset when it converts it,
        # so SQL Server sends it as ISO 8601 UTC text instead (style 127, e.g. 2026-08-12T19:24:38Z).
        # With the query option tds_fdw matches columns by exact name, so the aliases are lower case like
        # the unquoted Postgres column names.
        self.dbm.run_query("""
            DROP FOREIGN TABLE IF EXISTS {table};
            CREATE FOREIGN TABLE {table} (
                ErrorDtm TIMESTAMPTZ,
                ErrorDefinitionID INTEGER,
                MessageText TEXT
            ) SERVER {name}
            OPTIONS (query {query});
        """, {"table": self.table("error_notifications"), "name": self.name}, strings={
            "query": "SELECT CONVERT(varchar(40), ErrorDtm, 127) AS errordtm, "
                     "ErrorDefinitionID AS errordefinitionid, MessageText AS messagetext "
                     "FROM qry.ErrorNotifications"
        })
