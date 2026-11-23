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


from em_health.db_manager import DatabaseManager


class ForeignServer:
    """ Base class for a foreign server on one microscope and its foreign tables.
    The server is named <prefix>_<instrument id>. Its foreign tables live in the shared fdw schema,
    named <server>_<table> (e.g. fdw.ms_5_error_notifications), since every microscope needs its own:
    a foreign table belongs to exactly one foreign server.
    """
    prefix: str = ""

    def __init__(self, dbm: DatabaseManager, server: str, instr_id: int):
        self.dbm = dbm
        self.server = server
        self.instr_id = instr_id
        self.name = f"{self.prefix}_{instr_id}"
        self.setup_server()
        self.create_tables()

    def table(self, name: str) -> str:
        """ Foreign table of this server in the fdw schema, e.g. fdw.pg_5_event_property. """
        return f"fdw.{self.name}_{name}"

    def setup_server(self) -> None:
        raise NotImplementedError

    def create_tables(self) -> None:
        raise NotImplementedError
