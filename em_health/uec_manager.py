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

from datetime import datetime, timezone
from typing import Iterable, Optional

from em_health.db_manager import DatabaseManager
from em_health.db_client import MSClient
from em_health.utils.logs import logger


class UECManager:
    """ Manager class to operate on UEC (Alarms) data. """
    def __init__(self, dbname):
        self.dbname = dbname
        self.servers = None

    def get_servers(self):
        """ Get list of servers from the instruments table. """
        with DatabaseManager(self.dbname) as pgm:
            servers = pgm.run_query("SELECT server FROM public.instruments", mode="fetchall")
            self.servers = [s[0] for s in servers]
            if not self.servers:
                raise ValueError(f"{self.dbname} database has no servers in the instruments table")

    def get_metadata(self, server):
        """ Query UEC metadata from MSSQL DB."""
        with MSClient(db_name="DS", host=server) as msdb:
            metadata = ""
        return metadata

    def get_data(self, server):
        """ Query UEC data from MSSQL DB."""
        with MSClient(db_name="DS", host=server) as msdb:
            rows = msdb.run_query("""
                                SELECT 
                                    CAST(ErrorDtm AS DATETIME) AS ErrorDtm,
                                    MessageText,
                                    SubsystemID,
                                    DeviceTypeID,
                                    DeviceInstanceID,
                                    ErrorCodeID
                                FROM qry.ErrorNotifications
                            """, mode="fetchall")
            for row in rows:
                print(row)

    def import_metadata(self, server, metadata):
        """ Import UEC metadata into PostgreSQL DB. """
        with DatabaseManager(self.dbname) as db:
            db.run_query("""
                INSERT INTO public.uec_metadata (server, metadata)
                VALUES (%s, %s)
                ON CONFLICT (server)
                DO UPDATE SET metadata = EXCLUDED.metadata
            """, values=(server, metadata))

    def import_data(self, server, data):
        """ Import UEC data into PostgreSQL DB. """
        with DatabaseManager(self.dbname) as db:
            db.write_data(data, server, nocopy=True)

    def run_all_tasks(self):
        """ Import UEC data from MSSQL DB into PostgreSQL DB. """
        self.get_servers()
        for server in self.servers:
            metadata = self.get_metadata(server)
            self.import_metadata(server, metadata)
            data = self.get_data(server)
            self.import_data(server, data)
