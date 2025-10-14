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
import gzip
from datetime import datetime, timezone
import json
import xml.etree.ElementTree as ET  # https://github.com/lxml/lxml/blob/master/doc/performance.txt#L293
from typing import Iterable

from em_health.db_manager import DatabaseManager
from em_health.utils.tools import logger


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
        self.instrument_name = None
        self.db_name = None
        self.enum_values: dict[str, dict] = {}
        self.params: dict[int, dict] = {}

        if self.path.endswith('.xml.gz'):
            self.file = gzip.open(self.path, 'rb')
        else:
            self.file = open(self.path, 'rb')
        self.context = ET.iterparse(self.file, events=("end",))
        # Be aware, you have to parse XML sections in their order, i.e. enumerations first!

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
                self.instrument_name = m.get("name")
                self.db_name = m.get("type")
                if self.db_name not in ["tem", "sem"]:
                    raise ValueError(f"Database name {self.db_name} is not recognized")
                break
        if self.microscope is None:
            raise ValueError(f"Instrument '{instr_name}' not found in instruments.json")

    def parse_enumerations(self) -> None:
        """ Parse enumerations from xml. """
        get_instrument = True
        for event, elem in self.context:
            if self.__match(elem, "Enumerations"):
                for enum_elem in elem.findall('ns:Enumeration', namespaces=NS):
                    enum_name = enum_elem.get("Name")
                    if get_instrument:
                        instr_name = enum_elem.get("Instrument")
                        self.set_microscope(instr_name)
                        get_instrument = False
                    self.enum_values[enum_name] = {}

                    for literal in enum_elem.findall('ns:Literal', namespaces=NS):
                        literal_name = literal.get("Name")
                        literal_value = int(literal.text.strip())
                        self.enum_values[enum_name][literal_name] = literal_value

                elem.clear()
                break

        logger.info("Found %d enumerations", len(self.enum_values), extra={"prefix": self.instrument_name})
        logger.debug("Parsed enumerations:", extra={"prefix": self.instrument_name})
        logger.debug(json.dumps(self.enum_values, sort_keys=True, indent=2))

    def parse_parameters(self) -> None:
        """ Parse parameters from xml. """
        known_types = {
            'Int': 'int',
            'Float': 'float',
            'String': 'str',
            'Boolean': 'bool',
        }

        for event, elem in self.context:
            if self.__match(elem, "Instruments"):

                for instrument in elem.findall('ns:Instrument', namespaces=NS):
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
                                    "param_name": param.get("Name"),
                                    "enum_name": param.get("EnumerationName", None),
                                    "display_name": param.get("DisplayName"),
                                    "display_unit": param.get("DisplayUnit") or None,
                                    "storage_unit": param.get("StorageUnit") or None,
                                    "value_type": known_types.get(param.get("Type"), "str"),
                                    "event_id": param.get("EventID"),
                                    "event_name": param.get("EventName"),
                                    "abs_min": param.get("AbsoluteMinimum") or None,
                                    "abs_max": param.get("AbsoluteMaximum") or None
                                }

                    break  # only a single instrument is supported

                elem.clear()
                break

        logger.info("Found %d parameters", len(self.params), extra={"prefix": self.instrument_name})
        logger.debug("Parsed parameters:", extra={"prefix": self.instrument_name})
        logger.debug(json.dumps(self.params, sort_keys=True, indent=2))

    def parse_values(self,
                     instr_id: int,
                     params_dict: dict) -> Iterable[tuple]:
        """ Parse parameters values from XML.
        :param instr_id: instrument id from the instrument table
        :param params_dict: input parameters dict, here only used to fetch param type
        :return an Iterator of tuples
        """
        for event, elem in self.context:
            if self.__match(elem, "Values"):
                start, end = elem.get("Start"), elem.get("End")
                logger.info("Parsed values from %s to %s", start, end,
                            extra={"prefix": self.instrument_name})

            elif self.__match(elem, "ValueData"):
                param_id = int(elem.get("ParameterID"))
                param_dict = params_dict.get(param_id)
                if param_dict is None:
                    logger.error("Parameter ID %d not found, skipping", param_id,
                                 extra={"prefix": self.instrument_name})
                    elem.clear()  # clear skipped elements
                    continue
                value_type = param_dict["value_type"]

                param_values_elem = elem.find('ns:ParameterValues', namespaces=NS)
                if param_values_elem is not None:
                    for pval in param_values_elem.findall('ns:ParameterValue', namespaces=NS):
                        timestamp = self.__parse_ts_to_utc(pval.get("Timestamp"))
                        value_elem = pval.find('ns:Value', namespaces=NS)
                        value_text_raw = value_elem.text
                        value_num, value_text = self.__convert_value(param_id, value_text_raw, value_type)
                        if value_num is None and value_text is None:
                            continue  # failed to convert value

                        point = (timestamp, instr_id, param_id, value_num, value_text)
                        yield point

                elem.clear()  # Clear after handling <ValueData> and its children

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Close input XML file on exit. """
        if self.file:
            self.file.close()
            self.file = None
            self.context = None

    @staticmethod
    def __match(elem, name) -> bool:
        """ Strip namespace and match XML tag. """
        return elem.tag.endswith(f"}}{name}")

    @staticmethod
    def __parse_ts_to_utc(ts: str) -> datetime:
        """ Parse timestamp string into UTC.
        :param ts: input timestamp string
        """
        ts = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        return dt.astimezone(timezone.utc)

    @staticmethod
    def __convert_value(param_id: int,
                        value: str,
                        value_type: str):
        """ Convert the param value according to type.
        Returns value_num, value_text.
        """
        try:
            if value_type == "str":
                return None, str(value)
            elif value_type == "float":
                return float(value), None
            elif value_type == "int":  # works for int, IntEnum
                return int(value), None
            elif value_type == "bool":
                return int(value.strip() == "true"), None
            else:
                raise ValueError
        except (ValueError, TypeError):
            logger.error(f"Cannot convert '{value}' to {value_type} for param {param_id}")
            return None, None


def main(xml_fn, json_fn, nocopy):
    # Validate JSON file
    if not (os.path.exists(json_fn) and json_fn.endswith(".json")):
        logger.error("Settings file '%s' not found or is not a .json file.", json_fn)
        sys.exit(1)

    try:
        with open(json_fn, encoding="utf-8") as f:
            json_info = json.load(f)
            if not json_info:
                logger.error("Settings file '%s' is empty or invalid.", json_fn)
                sys.exit(1)
            logger.debug("Loaded json_info: %s", json_info)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse JSON file '%s': %s", json_fn, e)
        sys.exit(1)

    # Validate xml path
    if not os.path.exists(xml_fn):
        logger.error("Input xml not found: %s", xml_fn)
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

        with DatabaseManager(xmlparser.db_name,
                             username="emhealth",
                             password="POSTGRES_EMHEALTH_PASSWORD") as dbm:
            instrument_id = dbm.add_instrument(instr_dict)
            enum_ids = dbm.add_enumerations(instrument_id, xmlparser.enum_values)
            dbm.add_parameters(instrument_id, xmlparser.params, enum_ids)
            datapoints = xmlparser.parse_values(instrument_id, xmlparser.params)
            dbm.write_data(datapoints, nocopy=nocopy)
    else:
        logger.error("File %s has wrong format", xml_fn)
        sys.exit(1)
