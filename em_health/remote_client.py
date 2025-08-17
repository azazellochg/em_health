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

from em_health.db_manager import DatabaseManager


class RemoteDsDb(DatabaseManager):
    """ Connect to the PostgreSQL database on MPC. """
    def __init__(self, **kwargs):
        """ Redefine default connection settings. """
        super().__init__(port=60659, db_name="ds", **kwargs)

    def query_data(self, since: str = ''):
        """ Query raw data from Postgres. """
        query = """
            SELECT
                event_property_type_id AS param_id,
                event_dtm AS time,
                value_float,
                value_int,
                value_string,
                value_bool
            FROM core.event_property
            WHERE event_dtm > {since}
            ORDER BY event_dtm
        """
        return self.run_query(query,
                              strings={"since": since},
                              mode="fetchall")

    def query_parameters(self):
        """ Query parameters data from Postgres.
        A single event (id/name) can be used by multiple parameters (id/name).
        event_property_type_id is unique per instrument
        ept.event_type_id+ept.identifying_name must be unique
        """
        query = """
            SELECT
                ept.event_property_type_id AS param_id,
                ept.event_type_id AS event_id,
                et.identifying_name AS event_name,
                ept.identifying_name AS param_name,
                ept.label AS display_name,
                ept.display_units AS display_unit,
                ept.storage_units AS storage_unit,
                pt.identifying_name AS value_type,
                ept.absolute_min,
                ept.absolute_max,
                ept.caution_min,
                ept.caution_max,
                ept.warning_min,
                ept.warning_max,
                ept.critical_min,
                ept.critical_max,
                ept.is_active AS is_active
            FROM core.event_property_type ept
            JOIN core.parameter_type pt ON pt.parameter_type_id = ept.parameter_type_id
            JOIN core.event_type et ON et.event_type_id = ept.event_type_id
            ORDER BY ept.event_property_type_id
        """
        return self.run_query(query, mode="fetchall")


if __name__ == "__main__":
    db = RemoteDsDb(host="...", user="...", password="...")
    rows = db.query_data(since='2025-08-12')

    for row in rows:
        print(row)
