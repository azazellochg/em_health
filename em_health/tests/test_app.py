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
import unittest
from datetime import datetime as dt, timezone as tz

from em_health.utils.import_xml import ImportXML
from em_health.utils.tools import run_command
from em_health.db_manager import DatabaseManager

XML_FN = os.path.join(os.path.dirname(__file__), '9999_data.xml')
XML_FN2 = os.path.join(os.path.dirname(__file__), '9999_changed_data.xml')
JSON_INFO = [{
    "instrument": "9999, Test Instrument",
    "serial": 9999,
    "model": "Test instrument",
    "name": "Test",
    "type": "tem",
    "template": "krios",
    "server": "127.0.0.1"
}]
MANAGER = os.getenv("MANAGER_TYPE")


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

    def check_parameters(self, params: dict[int, dict]):
        self.assertEqual(len(params), 391)
        self.assertIn(171, params)
        self.assertEqual(params[184]["param_name"], "Laldwr")
        self.assertEqual(params[231]["display_name"], "Emission Current")
        self.assertEqual(params[400]["enum_name"], "CameraInsertStatus_enum")

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

    def test_client(self):
        # first import
        parser = ImportXML(XML_FN, JSON_INFO)
        parser.parse_enumerations()
        self.check_enumerations(parser.enum_values)
        parser.parse_parameters()
        self.check_parameters(parser.params)
        instr_dict = parser.get_microscope_dict()
        config_dict = {"params": parser.params, "enums": parser.enum_values}

        with DatabaseManager(parser.db_name) as dbm:
            # clean-up
            old = dbm.run_query("SELECT id FROM events.instruments WHERE serial=9999", mode="fetchone")
            if old:
                dbm.run_query("SELECT events.delete_instrument(%s)", values=(old[0],))

            instrument_id = dbm.add_instrument(instr_dict, config_dict)

            # convert to list since we need to iterate twice
            datapoints = list(parser.parse_values(instrument_id, parser.params))
            self.check_datapoints(datapoints)
            dbm.write_data(datapoints)

        run_command(f'{MANAGER} exec emhealth-db bash -c "pg_prove -d tem -U postgres /sql/tests/pgtap/04_import.sql"')

        # second import
        parser2 = ImportXML(XML_FN2, JSON_INFO)
        parser2.parse_enumerations()
        parser2.parse_parameters()
        instr_dict = parser2.get_microscope_dict()
        config_dict = {"params": parser2.params, "enums": parser2.enum_values}

        with DatabaseManager(parser2.db_name) as dbm:
            _ = dbm.add_instrument(instr_dict, config_dict)
            dbm.write_data(datapoints)

        run_command(f'{MANAGER} exec emhealth-db bash -c "pg_prove -d tem -U postgres /sql/tests/pgtap/05_import2.sql"')

    def atest_pgtap_tem(self):
        run_command(f'{MANAGER} exec emhealth-db bash -c "pg_prove -d tem -U postgres /sql/tests/pgtap/0[1-3]*.sql"')

    def atest_pgtap_sem(self):
        run_command(f'{MANAGER} exec emhealth-db bash -c "pg_prove -d sem -U postgres /sql/tests/pgtap/0[1-3]*.sql"')


if __name__ == '__main__':
    unittest.main()
