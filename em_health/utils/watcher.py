# **************************************************************************
# *
# * Authors:     Grigory Sharov (gsharov@mrc-lmb.cam.ac.uk) [1]
# *
# * [1] MRC Laboratory of Molecular Biology, MRC-LMB
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
import time
from watchdog.observers.polling import PollingObserver
from watchdog.events import PatternMatchingEventHandler

from em_health.utils.logs import logger


class FileWatcher:
    def __init__(self,
                 path: str,
                 json_fn: str,
                 stable_time: int = 10):
        """
        Watch for XML file creation and ensure a file is fully written before processing.
        :param path: Folder to watch
        :param json_fn: JSON file name
        """
        self.observer = PollingObserver(timeout=300)  # Poll every 300 s (5 min)
        self.path = path
        self.json_fn = json_fn
        self.stable_time = stable_time

    def start(self):
        """ Schedule watchdog for a specific file pattern. """
        event_handler = PatternMatchingEventHandler(
            patterns=["*_data.xml", "*_data.xml.gz"],
            ignore_patterns=[],
            ignore_directories=True
        )
        event_handler.on_modified = self.on_modify
        self.observer.schedule(event_handler, self.path, recursive=False)
        self.observer.start()
        logger.info("Watching %s for XML files (*_data.xml, *_data.xml.gz)... Press Ctrl+C to stop.", self.path)

        try:
            while self.observer.is_alive():
                self.observer.join(1)
        except KeyboardInterrupt:
            logger.info("Stopping watcher...")
            self.observer.stop()
        finally:
            self.observer.join()

    def on_modify(self, event):
        """ Log file modification and ensure a file is fully written before processing."""
        filepath = event.src_path
        logger.info("Detected modified file: %s", filepath)
        time.sleep(10)
        self.wait_until_complete(filepath)

    def wait_until_complete(self, filepath):
        """ Wait until file size is stable for self.stable_time seconds, then execute import. """
        last_size = -1
        unchanged_time = 0
        while True:
            try:
                size = os.path.getsize(filepath)
            except FileNotFoundError:
                logger.error("File %s disappeared", filepath)
                return

            if size == last_size and size > 0:
                unchanged_time += 1
                if unchanged_time >= self.stable_time:
                    logger.info("File complete: %s (%s bytes)", filepath, size)
                    # Call import_xml
                    from em_health.utils.import_xml import main as import_main
                    import_main(os.path.abspath(filepath),
                                os.path.abspath(self.json_fn),
                                True)
                    return
            else:
                unchanged_time = 0
                last_size = size

            time.sleep(3)


def main(input_path, json_fn):
    if not os.path.isdir(input_path):
        logger.error("Invalid directory: %s", input_path)
        sys.exit(1)

    # Validate JSON file
    if not (os.path.exists(json_fn) and json_fn.endswith(".json")):
        logger.error(f"Settings file '{json_fn}' not found or is not a .json file.")
        sys.exit(1)

    watcher = FileWatcher(path=input_path, json_fn=json_fn)
    watcher.start()
