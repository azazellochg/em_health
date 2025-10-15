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
import threading
from concurrent.futures import ThreadPoolExecutor
from watchdog.observers.polling import PollingObserver
from watchdog.events import PatternMatchingEventHandler

from em_health.utils.import_xml import main as import_main
from em_health.utils.tools import logger


class FileWatcher:
    def __init__(self,
                 path: str,
                 json_fn: str,
                 interval: int = None,
                 stable_time: int = None,
                 max_workers: int = None):
        """
        Watch for XML file creation and ensure a file is fully written before processing.
        :param path: Folder to watch
        :param json_fn: JSON file name
        :param interval: Polling interval in seconds
        :param stable_time: Number of times to check the file for the size change
        :param max_workers: Number of files to import in parallel
        """
        self.path = path
        self.json_fn = json_fn
        self.interval = interval or int(os.getenv("WATCH_INTERVAL", 300))
        self.stable_time = stable_time or int(os.getenv("WATCH_SIZE_COUNTER", 10))
        self.max_workers = max_workers or int(os.getenv("WATCH_MAX_WORKERS", 4))
        self.observer = PollingObserver(timeout=self.interval)
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.processed_files = set()
        self.lock = threading.Lock()

    def start(self):
        """Start the file watcher and process files as they appear."""
        event_handler = PatternMatchingEventHandler(
            patterns=["*_data.xml", "*_data.xml.gz"],
            ignore_directories=True
        )
        event_handler.on_created = self.on_file_detected
        event_handler.on_modified = self.on_file_detected

        self.observer.schedule(event_handler, self.path, recursive=False)
        self.observer.start()
        logger.info("Watching %s for XML files (*_data.xml, *_data.xml.gz)...", self.path)

        try:
            while self.observer.is_alive():
                self.observer.join(1)
        except KeyboardInterrupt:
            logger.info("Stopping watcher...")
            self.observer.stop()
        finally:
            self.executor.shutdown(wait=True)
            self.observer.join()

    def on_file_detected(self, event):
        filepath = event.src_path
        with self.lock:
            if filepath in self.processed_files:
                return
            self.processed_files.add(filepath)

        logger.info("Detected file: %s", filepath)
        threading.Thread(target=self._wait_and_submit, args=(filepath,), daemon=True).start()

    def _wait_and_submit(self, filepath: str):
        """Wait until file is stable, then submit for processing."""
        if self.wait_until_complete(filepath):
            self.executor.submit(self.process_file, filepath)

    def wait_until_complete(self, filepath: str) -> bool:
        """Wait until the file size is stable for self.stable_time seconds."""
        last_size = -1
        stable_counter = 0

        while stable_counter < self.stable_time:
            try:
                current_size = os.path.getsize(filepath)
            except FileNotFoundError:
                logger.warning("File disappeared: %s", filepath)
                return False

            if current_size == last_size:
                stable_counter += 1
            else:
                stable_counter = 0
                last_size = current_size

            time.sleep(3)

        logger.info("File is ready: %s (%d bytes)", filepath, last_size)
        return True

    def process_file(self, filepath: str):
        """Run the XML import on a ready file."""
        try:
            import_main(os.path.abspath(filepath),
                        os.path.abspath(self.json_fn),
                        nocopy=True)
        except Exception as e:
            logger.error("Error importing %s: %s", filepath, str(e))
        finally:
            with self.lock:
                self.processed_files.discard(filepath)


def main(input_path, json_fn, interval):
    if not os.path.isdir(input_path):
        logger.error("Invalid directory: %s", input_path)
        sys.exit(1)

    # Validate JSON file
    if not (os.path.exists(json_fn) and json_fn.endswith(".json")):
        logger.error("Settings file '%s' not found or is not a .json file.", json_fn)
        sys.exit(1)

    watcher = FileWatcher(path=input_path, json_fn=json_fn, interval=interval)
    watcher.start()
