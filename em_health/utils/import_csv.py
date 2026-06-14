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
from pathlib import Path
import gzip
from datetime import datetime, timezone
import json
from typing import Iterable

from em_health.db_manager import DatabaseManager
from em_health.utils.tools import logger


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

        if self.path.endswith('.log.gz'):
            self.file = gzip.open(self.path, 'rt')
        else:
            self.file = open(self.path, 'rt')

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
                break
        if self.microscope is None:
            raise ValueError(f"Instrument {serial} not found in instruments.json")

    def parse_values(self,
                     instr_id: int) -> Iterable[tuple]:
        """ Parse parameters values from CSV.
        :param instr_id: instrument id from the instrument table
        :return an Iterator of tuples
        """
        for line in self.file:
            if " ERROR " in line:
                level = "ERROR"
                pos = line.index("ERROR")

            elif " INFO " in line and "Temperature:" in line:
                level = "INFO"
                pos = line.index("INFO")

            else:
                continue

            # Example:
            # Sun Jun 14 00:02:24 2026
            ts_str = line[:24]
            timestamp = (
                datetime.strptime(ts_str, "%a %b %d %H:%M:%S %Y")
                .astimezone(timezone.utc)
            )
            message = line[pos + len(level):].strip()

            point = (timestamp, instr_id, level, message)
            yield point


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

        with DatabaseManager(csvparser.db_name,
                             username="emhealth",
                             password="POSTGRES_EMHEALTH_PASSWORD") as dbm:
            instrument_id = dbm.add_instrument(instr_dict)
            datapoints = csvparser.parse_values(instrument_id)
            for p in datapoints:
                print(p)
            #dbm.write_logs(datapoints, nocopy=nocopy)

        csvparser.file.close()
    else:
        logger.error("File %s has wrong format", csv_fn)
        sys.exit(1)
