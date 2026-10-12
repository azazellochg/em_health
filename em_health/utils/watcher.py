# **************************************************************************
# *
# * Authors:     Grigory Sharov (gsharov@mrclmb.ac.uk) [1]
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
# *  e-mail address 'gsharov@mrclmb.ac.uk'
# *
# **************************************************************************

import os
import sys
import signal
import threading
import argparse
from concurrent.futures import ThreadPoolExecutor
from watchdog.observers.polling import PollingObserver
from watchdog.events import PatternMatchingEventHandler

from em_health.utils.import_xml import main as import_main
from em_health.utils.tools import logger


class FileWatcher:
    def __init__(self,
                 path: str,
                 json_fn: str,
                 interval: int = 300,
                 stable_time: int = 10,
                 max_workers: int = 4):
        """
        Watch for XML files and import them once their size is stable.
        :param path: Directory to watch.
        :param json_fn: Path to the JSON settings file.
        :param interval: Watchdog polling interval in seconds.
        :param stable_time: Number of consecutive stable-size checks required.
        :param max_workers: Maximum number of concurrent XML imports.
        """
        self.path = path
        self.json_fn = json_fn
        self.interval = int(os.getenv("WATCH_INTERVAL", interval))
        self.stable_time = int(os.getenv("WATCH_SIZE_COUNTER", stable_time))
        self.max_workers = int(os.getenv("WATCH_MAX_WORKERS", max_workers))
        self.observer = PollingObserver(timeout=self.interval)
        self.stability_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="file-stability")
        self.import_executor = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="xml-import")
        self.in_progress = set()
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

    def start(self):
        """Run the observer until shutdown is requested or it fails."""
        event_handler = PatternMatchingEventHandler(
            patterns=["*_data.xml", "*_data.xml.gz"],
            ignore_directories=True
        )
        event_handler.on_created = self.on_file_detected
        event_handler.on_modified = self.on_file_detected
        observer_started = False

        try:
            self.observer.schedule(event_handler, self.path, recursive=False)
            self.observer.start()
            observer_started = True

            logger.info("Watching %s for XML files (*_data.xml, *_data.xml.gz)...", self.path)

            while not self.stop_event.is_set():
                self.observer.join(1)

                if not self.observer.is_alive() and not self.stop_event.is_set():
                    raise RuntimeError("Watchdog stopped unexpectedly")

        except Exception:
            logger.exception("Watchdog failed")
            raise

        finally:
            self.stop_event.set()

            try:
                if observer_started:
                    self.observer.stop()
                    self.observer.join()
            finally:
                try:
                    # Finish stability checks before shutting down the executor
                    self.stability_executor.shutdown(wait=True)
                finally:
                    # Allow submitted imports to finish
                    self.import_executor.shutdown(wait=True)

            logger.info("File watcher stopped")

    def request_stop(self):
        """Request graceful shutdown."""
        logger.info("Shutdown requested")
        self.stop_event.set()

    def on_file_detected(self, event):
        """Schedule a stability check for a newly detected or updated file."""
        if self.stop_event.is_set():
            return

        filepath = os.path.abspath(event.src_path)

        with self.lock:
            if self.stop_event.is_set() or filepath in self.in_progress:
                return

            self.in_progress.add(filepath)

        logger.info("Detected file: %s", filepath)

        try:
            self.stability_executor.submit(self._wait_and_submit,filepath)
        except RuntimeError:
            with self.lock:
                self.in_progress.discard(filepath)

            if not self.stop_event.is_set():
                logger.exception("Could not schedule stability check for %s", filepath)

    def _wait_and_submit(self, filepath: str):
        """Wait until a file is stable, then schedule its import."""
        submitted = False

        try:
            if self.wait_until_complete(filepath) and not self.stop_event.is_set():
                self.import_executor.submit(self.process_file,filepath)
                submitted = True

        except Exception:
            logger.exception("Failed to schedule import for %s", filepath)

        finally:
            if not submitted:
                with self.lock:
                    self.in_progress.discard(filepath)

    def wait_until_complete(self, filepath: str) -> bool:
        """
        Wait until file size remains stable for the configured
        number of consecutive checks.
        """
        last_size = -1
        stable_counter = 0

        while stable_counter < self.stable_time and not self.stop_event.is_set():
            try:
                current_size = os.path.getsize(filepath)

            except FileNotFoundError:
                logger.warning("File disappeared: %s", filepath)
                return False

            except OSError:
                logger.exception("Cannot access file %s", filepath)
                return False

            if current_size == last_size:
                stable_counter += 1
            else:
                stable_counter = 0
                last_size = current_size

            if self.stop_event.wait(3):
                return False

        if self.stop_event.is_set():
            return False

        logger.info("File is ready: %s (%d bytes)", filepath, last_size)
        return True

    def process_file(self, filepath: str):
        """Import a stable XML file."""
        try:
            import_main(filepath, self.json_fn)
        except Exception:
            logger.exception("Error importing %s", filepath)
        finally:
            # Permit subsequent updates to the same filename.
            with self.lock:
                self.in_progress.discard(filepath)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i",
        dest="input",
        required=True,
        help="Target directory with XML data files",
    )
    parser.add_argument(
        "-s",
        dest="settings",
        required=True,
        help="Path to instruments.json with metadata",
    )
    parser.add_argument(
        "-t",
        dest="interval",
        type=int,
        default=300,
        help="Polling interval in seconds (default: 300)",
    )

    args = parser.parse_args()

    input_path = os.path.abspath(args.input)
    json_fn = os.path.abspath(args.settings)

    if not os.path.isdir(input_path):
        logger.error("Invalid directory: %s", input_path)
        sys.exit(1)

    if not (os.path.isfile(json_fn) and json_fn.endswith(".json")):
        logger.error("Settings file '%s' not found or is not a .json file.", json_fn)
        sys.exit(1)

    try:
        watcher = FileWatcher(path=input_path, json_fn=json_fn, interval=args.interval)
    except (ValueError, OSError):
        logger.exception("Invalid watcher configuration")
        sys.exit(1)

    # Python signal handlers must be registered in the main thread.
    signal.signal(signal.SIGTERM, lambda signum, frame: watcher.request_stop())
    signal.signal(signal.SIGINT, lambda signum, frame: watcher.request_stop())

    watcher.start()


if __name__ == "__main__":
    main()
