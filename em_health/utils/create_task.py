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
import json

from em_health.utils.logs import logger

HM_EXE = r"C:\Program Files (x86)\Thermo Scientific Health Monitor\HealthMonitorCmd.exe"
TIME = "01:00"


class CreateTaskCmd:
    def __init__(self,
                 json_info: dict,
                 exe: str = HM_EXE):
        """ Initialize the class.
        :param json_info: dict with microscopes metadata
        :param exe: executable path
        """
        self.microscopes = json_info
        self.exe = exe

    def create_task(self):
        """ Create a daily task."""
        task_file = "export_hm_data.cmd"
        cmd = []
        for m in self.microscopes:
            instrument = m["instrument"]
            server = m["server"]
            xml_file = str(m["serial"]) + "_data.xml"
            cmd.append(f'"{self.exe}" -e -r 1 -t 1 -f {xml_file} -s {server} -i "{instrument}" --remove true')

        cmd.append('''
@REM -e export
@REM -r 1 last hour (2 - last day, 3 - last week, 4 - last two weeks, 5 - last month, 6 - last quarter, 7 - last year)
@REM -t 1 xml format
@REM -f output filename. Use full path to a shared network drive.
@REM -s DataServices server (hostname or IP of the microscope PC)
@REM -i "serial, model"
@REM -c settings.xml (if omitted, all settings are exported)
@REM --start "2025-05-22 13:01:00 +01:00" (will be ignored when -r is used)
@REM --end "2025-05-23 13:01:00 +01:00" (will be ignored when -r is used)
@REM --remove to overwrite old export files
''')

        with open(task_file, "w", encoding="utf-8") as f:
            f.write("\n".join(cmd))

        logger.info("Created file: %s\n"
                    "Create a task in the Task Scheduler on a Windows system with Health Monitor "
                    "to run the above script every hour. See documentation for details.",
                    os.path.abspath(task_file))


def main(exe, json_fn):
    # Validate JSON file
    if not (os.path.exists(json_fn) and json_fn.endswith(".json")):
        logger.error("Settings file '%s' not found or is not a .json file.", json_fn)
        sys.exit(1)

    try:
        with open(json_fn, encoding="utf-8") as f:
            json_info = json.load(f)
            if not json_info:
                logger.error("Settings file '%s' is empty or invalid", json_fn)
                sys.exit(1)
            logger.debug("Loaded json_info: %s", json_info)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse JSON file '%s': %s", json_fn, e)
        sys.exit(1)

    # Create a task
    cmd = CreateTaskCmd(json_info, exe)
    cmd.create_task()
