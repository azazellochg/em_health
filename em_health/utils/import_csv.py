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
import sys
import re
from pathlib import Path
import gzip
from datetime import datetime, timezone
import json
from typing import Iterable

from em_health.db_manager import DatabaseManager
from em_health.utils.tools import logger

TEMP_LINE_PATTERN = re.compile(
    r"""
    ^(?P<timestamp>.{24})\s+.*?\bINFO\b\s+
    (?P<camera>.+?)
    \s+Camera\ Temperature:\s*
    (?P<camera_temp>-?\d+(?:\.\d+)?)
    \s*C.*
    Proc\ Temperatures:\s*
    (?P<proc_temps>.+?)
    \s+C\s*$
    """,
    re.VERBOSE
)

ERROR_LINE_PATTERN = re.compile(
    r"""
    ^(?P<timestamp>.{24})\s+.*?\bERROR\b\s+
    (?P<message>.*)
    \s*$
    """,
    re.VERBOSE
)


class ImportCSV:
    def __init__(self,
                 path: str,
                 json_info: list[dict]):
        """ Initialize the class.
        :param path: Path to an CSV log file
        :param json_info: list of dictionaries with microscope metadata
        """
        self.path = path
        self.json_info = json_info
        self.microscope = None
        self.instrument_name = None
        self.db_name = None
        self.params: dict[int, dict] = {}
        self.camera_name = "GatanCamera"
        self.proc_count = 0

        if self.path.endswith(".log.gz"):
            self.file = gzip.open(self.path, "rt")
        else:
            self.file = open(self.path, "rt")

        self.set_microscope()

    def get_microscope_dict(self) -> dict:
        """ Return microscope dictionary. """
        if self.microscope is None:
            raise ValueError("Microscope dict is not defined")
        return self.microscope

    def set_microscope(self) -> None:
        """ Set microscope and db_name using JSON settings. """
        try:
            serial = int(Path(self.path).name.split(".")[0])
        except ValueError:
            raise ValueError("Invalid CSV log file name")

        for m in self.json_info:
            if m.get("serial") == serial:
                self.microscope = m
                self.instrument_name = m.get("name")
                self.db_name = m.get("type")
                if self.db_name not in ["tem", "sem"]:
                    raise ValueError(f"Database name {self.db_name} is not recognized")
                return

        raise ValueError(f"Instrument {serial} not found in instruments.json")

    @staticmethod
    def _parse_timestamp(ts: str) -> datetime:
        return (
            datetime.strptime(
                ts,
                "%a %b %d %H:%M:%S %Y"
            )
            .replace(tzinfo=timezone.utc)
        )

    @classmethod
    def _parse_temperature_match(cls, match):
        return (
            cls._parse_timestamp(match.group("timestamp")),
            float(match.group("camera_temp")),
            map(float, match.group("proc_temps").split())
        )

    @classmethod
    def _parse_error_match(cls, match):
        return (
            cls._parse_timestamp(match.group("timestamp")),
            match.group("message")
        )

    def parse_parameters(self):
        """ Parse the first line with temperatures to get parameter names. """
        for line in self.file:
            if 'Camera Temperature' in line:
                logger.debug("Parsing line: %s", line)
                match = TEMP_LINE_PATTERN.match(line)
                self.camera_name = match.group("camera").strip().replace(" ", "")
                self.proc_count = len(match.group("proc_temps").split())
                break

        self.file.seek(0)

        next_id = 10000

        for proc_id in range(1, self.proc_count + 1):
            self.params[next_id] = {
                "subsystem": "AcquisitionServer",
                "component": self.camera_name,
                "param_name": f"Processor{proc_id}Temperature",
                "enum_name": None,
                "display_name": f"Processor {proc_id} temperature",
                "display_unit": "°C",
                "storage_unit": "°C",
                "value_type": "float",
                "event_id": 0,
                "event_name": "ProcTemperatureChangedEvent",
                "abs_min": -50.0,
                "abs_max": 180.0,
            }
            next_id += 1

        self.params[next_id] = {
            "subsystem": "AcquisitionServer",
            "component": self.camera_name,
            "param_name": "CameraTemperature",
            "enum_name": None,
            "display_name": "Camera temperature",
            "display_unit": "°C",
            "storage_unit": "°C",
            "value_type": "float",
            "event_id": 0,
            "event_name": "CameraTemperatureChangedEvent",
            "abs_min": -50.0,
            "abs_max": 180.0,
        }

        self.params[next_id + 1] = {
            "subsystem": "AcquisitionServer",
            "component": self.camera_name,
            "param_name": "CameraError",
            "enum_name": None,
            "display_name": "Camera error message",
            "display_unit": None,
            "storage_unit": None,
            "value_type": "str",
            "event_id": 0,
            "event_name": "CameraErrorEvent",
            "abs_min": None,
            "abs_max": None,
        }

        self.processor_param_names = [
            f"Processor{i}Temperature"
            for i in range(1, self.proc_count + 1)
        ]

        logger.info("Found %d parameters", len(self.params), extra={"prefix": self.instrument_name})
        logger.debug("Parsed parameters:", extra={"prefix": self.instrument_name})
        logger.debug(json.dumps(self.params, sort_keys=True, indent=2))

    def parse_values(self,
                     instr_id: int,
                     param_ids: dict) -> Iterable[tuple]:
        """ Parse parameters values from XML.
        :param instr_id: instrument id from the instrument table
        :param param_ids: input parameters dict
        :return an Iterator of tuples
        """
        camera_param_id = param_ids["CameraTemperature"]
        error_param_id = param_ids["CameraError"]

        proc_param_ids = [
            param_ids[f"Processor{i}Temperature"]
            for i in range(1, self.proc_count + 1)
        ]

        for line in self.file:
            if "Camera Temperature" in line:
                match = TEMP_LINE_PATTERN.match(line)
                if match:
                    (
                        timestamp,
                        camera_temp,
                        proc_temps,
                    ) = self._parse_temperature_match(match)

                    yield (
                        timestamp,
                        instr_id,
                        camera_param_id,
                        camera_temp,
                        None,
                    )

                    yield from (
                        (
                            timestamp,
                            instr_id,
                            param_id,
                            temp,
                            None,
                        )
                        for param_id, temp
                        in zip(proc_param_ids, proc_temps)
                    )

            elif "ERROR" in line:
                match = ERROR_LINE_PATTERN.match(line)

                if match:
                    timestamp, message = self._parse_error_match(match)
                    yield (
                        timestamp,
                        instr_id,
                        error_param_id,
                        None,
                        message,
                    )


def main(csv_fn, json_fn, nocopy):
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

    # Validate CSV path
    if not os.path.exists(csv_fn):
        logger.error("Input CSV not found: %s", csv_fn)
        sys.exit(1)

    _, extension = os.path.splitext(csv_fn)

    if extension in [".log", ".gz"]:
        if extension == ".gz":
            with open(csv_fn, 'rb') as f:
                magic = f.read(2)
            if magic != b'\x1f\x8b':
                raise IOError("Input file is not GZIP type!")

        csvparser = ImportCSV(csv_fn, json_info)
        instr_dict = csvparser.get_microscope_dict()
        csvparser.parse_parameters()

        param_ids = {
            v["param_name"]: k
            for k, v in csvparser.params.items()
        }

        with DatabaseManager(csvparser.db_name,
                             username="emhealth",
                             password="POSTGRES_EMHEALTH_PASSWORD") as dbm:

            instrument_id = dbm.add_instrument(instr_dict)
            dbm.add_parameters(instrument_id, csvparser.params, {})
            datapoints = csvparser.parse_values(instrument_id, param_ids)
            dbm.write_data(datapoints, nocopy=nocopy)

        csvparser.file.close()
    else:
        logger.error("File %s has wrong format", csv_fn)
        sys.exit(1)
