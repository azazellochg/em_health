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

from em_health.utils.import_xml import ImportXML
from em_health.db_manager import DatabaseManager

XML_FN = os.path.join(os.path.dirname(__file__), '0000_data.xml')
JSON_INFO = [{
    "instrument": "9999, Test Instrument",
    "serial": 9999,
    "model": "Test instrument",
    "name": "Test",
    "type": "tem",
    "template": "krios",
    "server": "127.0.0.1"
}]


class TestXMLImport(unittest.TestCase):

    def run_test_query(self,
                       dbm: DatabaseManager,
                       query: str,
                       values: tuple,
                       expected_result: int | str,
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
            ("2025-07-28 10:48:42.685000+0", "347"): "5.602248",
            ("2025-07-28 11:24:02.283000+0", "93"): "2"
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

        self.run_test_query(dbm, "SELECT COUNT(*) FROM public.parameters WHERE instrument_id = %s",
                            (instrument_id,), 391)

        self.run_test_query(dbm, "SELECT param_name FROM public.parameters WHERE instrument_id = %s AND param_id=%s",
                            (instrument_id, 184), "Laldwr")

        self.run_test_query(dbm, "SELECT COUNT(*) FROM public.data WHERE instrument_id = %s",
                            (instrument_id,), 1889)

        self.run_test_query(dbm, "SELECT COUNT(*) FROM public.data WHERE instrument_id = %s and time > %s",
                            (instrument_id, "2025-07-28 11:00:00+0"), 1333)
        print("[OK] database test")

    def test_parsing(self):
        parser = ImportXML(XML_FN, JSON_INFO)

        parser.parse_enumerations()
        self.check_enumerations(parser.enum_values)

        parser.parse_parameters()
        self.check_parameters(parser.params)

        instr_dict = parser.get_microscope_dict()

        with DatabaseManager(parser.db_name) as dbm:
            instrument_id, instrument_name = dbm.add_instrument(instr_dict)
            enum_ids = dbm.add_enumerations(instrument_id, parser.enum_values, instrument_name)
            dbm.add_parameters(instrument_id, parser.params, enum_ids, instrument_name)

            # convert to list since we need to iterate twice
            datapoints = list(parser.parse_values(instrument_id, parser.params, instrument_name))

            self.check_datapoints(datapoints)

            dbm.write_data(datapoints, instrument_name, nocopy=True)
            self.check_db(dbm, instrument_id)
            #dbm.clean_instrument_data(instrument_serial=9999)


if __name__ == '__main__':
    unittest.main()
