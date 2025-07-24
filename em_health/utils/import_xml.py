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

import os
import sys
import argparse
import gzip
from datetime import datetime, timezone
import json
import xml.etree.ElementTree as ET
from typing import Iterable

from em_health.db_manager import DatabaseManager
from em_health.utils.logs import DEBUG, logger


NS = {'ns': 'HealthMonitorExport http://schemas.fei.com/HealthMonitor/Export/2009/07'}


class ImportXML:
    def __init__(self,
                 path: str,
                 json_info: list[dict]):
        """ Initialize the class.
        :param path: Path to an XML file
        :param json_info: list of dictionaries with microscope metadata
        """
        self.path = path
        self.json_info = json_info
        self.microscope = None
        self.db_name = None
        self.enumerations: dict[str, dict] = {}
        self.params: dict[int, dict] = {}

        if self.path.endswith('.xml.gz'):
            self.file = gzip.open(self.path, 'rb')
        else:
            self.file = open(self.path, 'rb')
        self.context = ET.iterparse(self.file, events=("end",))

    def get_microscope_dict(self) -> dict:
        """ Return microscope dictionary. """
        if self.microscope is None:
            raise ValueError("Microscope dict is not defined")
        return self.microscope

    def set_microscope(self, instr_name: str) -> None:
        """ Set microscope and db_name using JSON settings. """
        for m in self.json_info:
            if m.get("instrument") == instr_name:
                self.microscope = m
                self.db_name = m.get("type")
                if self.db_name not in ["tem", "sem"]:
                    raise ValueError(f"Database name {self.db_name} is not recognized")
                break
        if self.microscope is None:
            raise ValueError(f"Instrument '{instr_name}' not found in settings.json")

    def parse_enumerations(self) -> None:
        """ Parse enumerations from xml. """
        for event, elem in self.context:
            if self.__match(elem, "Enumerations"):
                for enum_elem in elem.findall('ns:Enumeration', namespaces=NS):
                    enum_name = enum_elem.get("Name")
                    self.enumerations[enum_name] = {}

                    for literal in enum_elem.findall('ns:Literal', namespaces=NS):
                        literal_name = literal.get("Name")
                        literal_value = int(literal.text.strip())
                        self.enumerations[enum_name][literal_name] = literal_value

                elem.clear()
                break

        if DEBUG:
            logger.debug("Parsed enumerations:")
            for e in self.enumerations.items():
                logger.debug(e)

    def parse_parameters(self) -> None:
        """ Parse parameters from xml. """
        known_types = {
            'Int': 'int',
            'Float': 'float',
            'String': 'str'
        }

        for event, elem in self.context:
            if self.__match(elem, "Instruments"):

                for instrument in elem.findall('ns:Instrument', namespaces=NS):
                    instr_name = instrument.get("Name")
                    self.set_microscope(instr_name)

                    for subsystem in instrument.findall('ns:Component', namespaces=NS):
                        subsystem_name = subsystem.get("Name")

                        for component in subsystem.findall('ns:Component', namespaces=NS):
                            component_name = component.get("Name", None)

                            for param in component.findall('ns:Parameter', namespaces=NS):
                                param_id = int(param.get("ID"))

                                # None is used because we want to avoid storing empty strings
                                self.params[param_id] = {
                                    "subsystem": subsystem_name,
                                    "component": component_name,
                                    "name": param.get("Name"),
                                    "display_name": param.get("DisplayName"),
                                    "type": known_types[param.get("Type")],
                                    #"event_id": param.get("EventID"),
                                    #"event_name": param.get("EventName"),
                                    "enum": param.get("EnumerationName", None),
                                    "storage_unit": param.get("StorageUnit") or None,
                                    "display_unit": param.get("DisplayUnit") or None,
                                    "display_scale": param.get("DisplayScale") or None,  # Log or Linear
                                    #"format_string": param.get("FormatString"),  # F1 or F2
                                    #"max_log_interval": param.get("MaxLogInterval"),
                                    #"abs_min": param.get("AbsoluteMinimum"),
                                    #"abs_max": param.get("AbsoluteMaximum"),
                                }

                    break  # only a single instrument is supported

                elem.clear()
                break

        if DEBUG:
            logger.debug("Parsed parameters:")
            for p in sorted(self.params.keys()):
                logger.debug(f"{p}: {self.params[p]}")

    def parse_values(self,
                     instr_id: int,
                     params_dict: dict) -> Iterable[str]:
        """ Parse parameters values from XML.
        :param instr_id: instrument id from the instrument table
        :param params_dict: input parameters dict, here only used to fetch param type
        :return an Iterator of tuples
        """
        for event, elem in self.context:
            if self.__match(elem, "Values"):
                start, end = elem.get("Start"), elem.get("End")
                logger.info("Parsed values from %s to %s", start, end)

            elif self.__match(elem, "ValueData"):
                param_id = int(elem.get("ParameterID"))
                param_dict = params_dict.get(param_id)
                if param_dict is None:
                    logger.error("Parameter %d not found, skipping", param_id)
                    elem.clear() # clear skipped elements
                    continue
                value_type = param_dict["type"]
                instr_id = str(instr_id)
                param_id = str(param_id)

                param_values_elem = elem.find('ns:ParameterValues', namespaces=NS)
                if param_values_elem is not None:
                    for pval in param_values_elem.findall('ns:ParameterValue', namespaces=NS):
                        timestamp = self.__parse_ts_to_utc(pval.get("Timestamp"))
                        value_elem = pval.find('ns:Value', namespaces=NS)
                        value_text_raw = value_elem.text
                        value_num, value_text = self.__convert_value(param_id, value_text_raw, value_type)

                        # all values must be strings
                        point = "\t".join([timestamp, instr_id, param_id, value_num, value_text])
                        yield point

                elem.clear()  # Clear after handling <ValueData> and its children

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Close input XML file on exit. """
        if self.file:
            self.file.close()
            self.file = None

    @staticmethod
    def __match(elem, name) -> bool:
        """ Strip namespace and match XML tag. """
        return elem.tag.endswith(f"}}{name}")

    @staticmethod
    def __parse_ts_to_utc(ts: str) -> str:
        """ Parse timestamp string into UTC ISO 8601 string as expected by COPY.
        Removes colon from the timezone, e.g.:
        "2025-05-18T10:39:36.982+01:00" → "2025-05-18T10:39:36.982+0100"
        :param ts: input timestamp string
        """
        ts_fixed = ts[:-3] + ts[-2:]
        time_formats = [
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%fZZ",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
        ]
        for time_format in time_formats:
            try:
                dt_local = datetime.strptime(ts_fixed, time_format)
                dt_utc = dt_local.astimezone(timezone.utc)
                # PostgreSQL COPY expects 'YYYY-MM-DD HH:MM:SS.sss+00'
                return dt_utc.strftime("%Y-%m-%d %H:%M:%S.%f%z")[:-3]
            except ValueError:
                continue

        raise ValueError(f"Unsupported time format: {ts}")

    @staticmethod
    def __convert_value(param_id: str,
                        value: str,
                        value_type: str):
        """ Convert the param value according to type.
        Returns value_num, value_text as strings.
        """
        try:
            if value_type == "str":
                return '\\N', str(value)
            elif value_type == "float":
                return str(value), '\\N'
            elif value_type == "int":  # works for int and IntEnum
                return str(int(value)), '\\N'
            else:
                raise ValueError
        except (ValueError, TypeError):
            raise ValueError(f"Cannot convert '{value}' to {value_type} for param {param_id}")


def main(argv=None):
    description = """
    Import health monitor data to TimescaleDB. Only XML format is supported.
    Examples: 
        import_xml -i path/to/data.xml.gz -s path/to/settings.json
        import_xml -i path/to/data.xml -s path/to/settings.json
    """
    parser = argparse.ArgumentParser(description=description,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", dest="input", required=True,
                        help="Path to XML file (.xml or .xml.gz)")
    parser.add_argument("-s", dest="settings", required=True,
                        help="Path to settings.json with microscopes metadata")
    args = parser.parse_args(argv)
    xml_fn = args.input
    json_fn = args.settings

    # Validate JSON file
    if not (os.path.exists(json_fn) and json_fn.endswith(".json")):
        logger.error(f"Settings file '{json_fn}' not found or is not a .json file.")
        sys.exit(1)

    try:
        with open(json_fn, encoding="utf-8") as f:
            json_info = json.load(f)
            if not json_info:
                logger.error(f"Settings file '{json_fn}' is empty or invalid.")
                sys.exit(1)
            logger.debug("Loaded json_info: %s", json_info)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON file '{json_fn}': {e}")
        sys.exit(1)

    # Validate xml path
    if not os.path.exists(xml_fn):
        logger.error(f"Input xml not found: {xml_fn}")
        sys.exit(1)

    _, extension = os.path.splitext(xml_fn)

    if extension in [".xml", ".gz"]:
        if extension == ".gz":
            with open(xml_fn, 'rb') as f:
                magic = f.read(2)
            if magic != b'\x1f\x8b':
                raise IOError("Input file is not GZIP type!")

        xmlparser = ImportXML(xml_fn, json_info)
        xmlparser.parse_enumerations()
        xmlparser.parse_parameters()
        instr_dict = xmlparser.get_microscope_dict()

        with DatabaseManager(xmlparser.db_name) as dc:
            instrument_id = dc.add_instrument(instr_dict)
            enums_dict = dc.add_enumerations(instrument_id, xmlparser.enumerations)
            dc.add_parameters(instrument_id, xmlparser.params, enums_dict)
            datapoints = xmlparser.parse_values(instrument_id, xmlparser.params)
            dc.write_data(datapoints)
            #if DEBUG:
            #    for p in datapoints:
            #        print(p)
    else:
        logger.error("File %s has wrong format", xml_fn)
        sys.exit(1)


if __name__ == '__main__':
    main()
