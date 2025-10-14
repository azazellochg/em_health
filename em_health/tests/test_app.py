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

import os.path
import unittest
from datetime import datetime as dt, timezone as tz

from em_health.utils.import_xml import ImportXML
from em_health.utils.tools import run_command
from em_health.db_manager import DatabaseManager

XML_FN = os.path.join(os.path.dirname(__file__), '9999_data.xml')
JSON_INFO = [{
    "instrument": "9999, Test Instrument",
    "serial": 9999,
    "model": "Test instrument",
    "name": "Test",
    "type": "tem",
    "template": "krios",
    "server": "127.0.0.1"
}]


class TestEMHealth(unittest.TestCase):

    def run_test_query(self,
                       dbm: DatabaseManager,
                       query: str,
                       values: tuple,
                       expected_result: int | str | float,
                       do_return: bool = False):
        result = dbm.run_query(query, values=values, mode="fetchone")
        if do_return:
            # ignore expected_result
            return result[0]
        else:
            self.assertEqual(result[0], expected_result)

    def check_enumerations(self, enums: dict[str, dict]):
        self.assertEqual(len(enums), 41)
        self.assertEqual(enums["MicroscopeType"]["Tecnai"], 2)
        self.assertEqual(enums["VacuumState_enum"]["AllVacuumColumnValvesClosed"], 6)
        self.assertEqual(len(enums["FegState_enum"]), 8)
        print("[OK] enumerations test")

    def check_parameters(self, params: dict[int, dict]):
        self.assertEqual(len(params), 391)
        self.assertIn(171, params)
        self.assertEqual(params[184]["param_name"], "Laldwr")
        self.assertEqual(params[231]["display_name"], "Emission Current")
        self.assertEqual(params[400]["enum_name"], "CameraInsertStatus_enum")
        print("[OK] parameters test")

    def check_datapoints(self, points: list[tuple]):
        expected = {
            (dt(2025,7,28,10,48,42,685000, tzinfo=tz.utc), 347): 5.602248,
            (dt(2025,7,28,11,24,2,283000,tzinfo=tz.utc), 93): 2
        }

        match_count = 0
        for p in points:
            key = (p[0], p[2])
            if key in expected:
                self.assertEqual(p[3], expected[key])
                match_count += 1

        self.assertEqual(match_count, 2)
        print("[OK] datapoints test")

    def check_db(self, dbm: DatabaseManager, instrument_id: int):
        self.run_test_query(dbm, "SELECT model FROM public.instruments WHERE serial = %s",
                            (9999,), "Test instrument")

        self.run_test_query(dbm, "SELECT COUNT(id) FROM public.enum_types WHERE instrument_id= %s",
                            (instrument_id,), 41)

        eid = self.run_test_query(dbm, "SELECT id FROM public.enum_types WHERE instrument_id = %s AND name= %s",
                                  (instrument_id, "FegState_enum"), expected_result=-1, do_return=True)

        self.run_test_query(dbm, "SELECT value FROM public.enum_values WHERE enum_id = %s AND member_name = %s",
                            (eid, "Operate"), 4)

        self.run_test_query(dbm, "SELECT COUNT(*) FROM public.enum_values WHERE enum_id = %s",
                            (eid,), 8)

        self.run_test_query(dbm, "SELECT COUNT(*) FROM public.parameters WHERE instrument_id = %s",
                            (instrument_id,), 391)

        self.run_test_query(dbm, "SELECT param_name FROM public.parameters WHERE instrument_id = %s AND param_id=%s",
                            (instrument_id, 184), "Laldwr")

        self.run_test_query(dbm, "SELECT enum_id FROM public.parameters WHERE instrument_id = %s AND param_name = %s",
                            (instrument_id, "FegState",), eid)

        self.run_test_query(dbm, "SELECT COUNT(*) FROM public.data WHERE instrument_id = %s",
                            (instrument_id,), 1889)

        self.run_test_query(dbm, "SELECT COUNT(*) FROM public.data WHERE instrument_id = %s and time > %s",
                            (instrument_id, "2025-07-28 11:00:00+0"), 1333)
        print("[OK] database test #1")

    def check_db2(self, dbm: DatabaseManager, instrument_id: int):
        # check updated enums and history table
        eid = self.run_test_query(dbm, "SELECT id FROM public.enum_types WHERE instrument_id = %s AND name= %s",
                                  (instrument_id, "FegState_enum"), expected_result=-1, do_return=True)

        self.run_test_query(dbm, "SELECT value FROM public.enum_values WHERE enum_id = %s AND member_name = %s",
                            (eid, "Operate"), 99)
        self.run_test_query(dbm, "SELECT value FROM public.enum_values WHERE enum_id = %s AND member_name = %s",
                            (eid, "Standby"), 100)
        self.run_test_query(dbm, "SELECT value FROM public.enum_values_history WHERE enum_id = %s AND member_name = %s",
                            (eid, "Operate"), 4)
        self.run_test_query(dbm, "SELECT value FROM public.enum_values_history WHERE enum_id = %s AND member_name = %s",
                            (eid, "Standby"), 5)

        # check updated params and history table
        self.run_test_query(dbm, "SELECT abs_min FROM public.parameters WHERE instrument_id = %s AND param_id=%s",
                            (instrument_id, 351), 250.5)
        self.run_test_query(dbm, "SELECT abs_min FROM public.parameters_history WHERE instrument_id = %s AND param_id=%s",
                            (instrument_id, 351), 273.15)

        print("[OK] database test #2")

    @staticmethod
    def modify_input(enums: dict[str, dict],
                     params: dict[int, dict]):
        enums["FegState_enum"]["Operate"] = 99
        enums["FegState_enum"]["Standby"] = 100
        params[351]["abs_min"] = 250.5

    def test_client(self):
        """ Test XML parser and the db client."""
        parser = ImportXML(XML_FN, JSON_INFO)
        parser.parse_enumerations()
        self.check_enumerations(parser.enum_values)
        parser.parse_parameters()
        self.check_parameters(parser.params)

        instr_dict = parser.get_microscope_dict()

        with DatabaseManager(parser.db_name,
                             username="emhealth",
                             password="POSTGRES_EMHEALTH_PASSWORD") as dbm:
            # first import
            instrument_id = dbm.add_instrument(instr_dict)
            enum_ids = dbm.add_enumerations(instrument_id, parser.enum_values)
            dbm.add_parameters(instrument_id, parser.params, enum_ids)

            # convert to list since we need to iterate twice
            datapoints = list(parser.parse_values(instrument_id, parser.params))
            self.check_datapoints(datapoints)

            dbm.write_data(datapoints)
            self.check_db(dbm, instrument_id)

            # modify enums and params
            self.modify_input(parser.enum_values, parser.params)

            # second import
            instrument_id = dbm.add_instrument(instr_dict)
            enum_ids = dbm.add_enumerations(instrument_id, parser.enum_values)
            dbm.add_parameters(instrument_id, parser.params, enum_ids)

            self.check_datapoints(datapoints)
            dbm.write_data(datapoints, nocopy=True)
            self.check_db2(dbm, instrument_id)

            # clean-up
            dbm.clean_instrument_data(instrument_serial=9999)

    def test_pgtap(self):
        """ Run database tests with pgTAP. """
        run_command('docker exec timescaledb bash -c "pg_prove -d tem -U postgres /sql/tests/pgtap/*.sql"')

if __name__ == '__main__':
    unittest.main()
