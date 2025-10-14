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

import logging
import os
import time
from functools import wraps
import subprocess

try:
    from memory_profiler import memory_usage
    HAS_MEMORY_PROFILER = True
except ImportError:
    HAS_MEMORY_PROFILER = False

DEBUG = os.getenv("EMHEALTH_DEBUG", "false").lower() in ("true", "1", "yes")


class PrefixFormatter(logging.Formatter):
    def format(self, record):
        prefix = getattr(record, "prefix", "")
        record.message = record.getMessage()

        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)

        if prefix:
            record.message = f"[{prefix}] {record.message}"

        return self.formatMessage(record)


logger = logging.getLogger(__name__)
fmt = PrefixFormatter(
    fmt='[%(levelname)s] %(asctime)s %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S'
)

file_handler = logging.FileHandler("emhealth.log", mode="a", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(fmt)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(fmt)

logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)
logger.handlers = []  # Clear any existing handlers
logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def profile(fn):
    """ Decorator for profiling functions.
    See https://hakibenita.com/fast-load-data-python-postgresql
    """
    @wraps(fn)
    def inner(*args, **kwargs):
        fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
        print(f'\n{fn.__name__}({fn_kwargs_str})')

        # Measure time and memory together, only once
        t0 = time.perf_counter()

        def _target():
            retval[0] = fn(*args, **kwargs)

        from threading import Thread
        retval = [None]
        thread = Thread(target=_target)
        thread.start()
        if HAS_MEMORY_PROFILER:
            mem_usage = memory_usage((lambda: thread.join()), interval=0.1, timeout=None)
        elapsed = time.perf_counter() - t0

        print(f'Time   {elapsed:.4f} s')
        if HAS_MEMORY_PROFILER:
            print(f'Memory {max(mem_usage) - min(mem_usage):.2f} MB')
        return retval[0]

    return inner


def run_command(command: str, capture_output: bool = False, check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command with logging."""
    logger.info("Running command: %s", command)
    return subprocess.run(command, shell=True, check=check, capture_output=capture_output, text=True)
