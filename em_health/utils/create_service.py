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

from em_health.utils.tools import logger


def main(script_fn: str,
         service_fn: str,
         desc: str,
         restart: int = 10):
    """ Create systemd service file for the current user.
    :param script_fn: Python script to run
    :param service_fn: output systemd filename
    :param desc: service description
    :param restart: restart service every N seconds
    """
    script_path = Path(__file__).resolve()
    project_dir = script_path.parents[2]
    env_file = project_dir / "docker" / ".env"
    script = script_path.parent / script_fn
    python_path = Path(sys.executable).resolve()
    systemd_dir = Path.home() / ".config" / "systemd" / "user"
    service_file = systemd_dir / service_fn

    systemd_dir.mkdir(parents=True, exist_ok=True)
    content = f"""
[Unit]
Description={desc}
After=network.target

[Service]
Type=simple
EnvironmentFile={env_file}
ExecStart={python_path} {script}
WorkingDirectory={project_dir}

Restart=on-failure
RestartSec={restart}

[Install]
WantedBy=default.target

"""

    with open(service_file, "w") as f:
        f.write(content)

    logger.info("Created file: %s\n"
                "Run:\n\tsystemctl --user daemon-reload\n"
                "\tsystemctl --user enable --now %s",
                os.path.abspath(service_file),
                service_fn)
