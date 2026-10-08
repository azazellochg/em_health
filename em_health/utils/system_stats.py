#!/usr/bin/env python3
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
import platform
import subprocess
import fcntl
import time
import json
from datetime import datetime, timezone
from pathlib import Path
from psycopg.types.json import Jsonb

from em_health.db_analyze import DatabaseAnalyzer
from em_health.utils.tools import logger

FAST_INTERVAL = 15  # 15 sec
SLOW_INTERVAL = 900  # 15 min
DB_RETRY_INTERVAL = 10
DB_MAX_ATTEMPTS = 5

MANAGER = os.getenv("MANAGER_TYPE", "podman")
if MANAGER not in ["docker", "podman"]:
    raise RuntimeError(f"Unsupported container manager: {MANAGER}")

CPU_MODES = ("user", "system", "idle", "iowait")
MEMORY_METRICS = {
    "MemTotal": "node_memory_MemTotal_bytes",
    "MemFree": "node_memory_MemFree_bytes",
    "Buffers": "node_memory_Buffers_bytes",
    "Cached": "node_memory_Cached_bytes",
    "SReclaimable": "node_memory_SReclaimable_bytes"
}
ALLOWED_FSTYPES = [
    "ext4",
    "xfs",
    "btrfs",
    "zfs"
]
CONTAINERS = [
    "emhealth-db",
    "emhealth-grafana",
    "emhealth-renderer"
]


# ------ Metrics funcs ----------------------------------------------------------------------------
def read_loadavg(metrics):
    """ Read current CPU load avg. """
    with open("/proc/loadavg", "r") as f:
        parts = f.read().split()

    if len(parts) < 3:
        logger.error("Unexpected content in /proc/loadavg")
        return

    metrics.append({
        "metric": "node_load1",
        "value": float(parts[0])
    })

    metrics.append({
        "metric": "node_load5",
        "value": float(parts[1])
    })

    metrics.append({
        "metric": "node_load15",
        "value": float(parts[2])
    })


def read_cpu_seconds(metrics):
    """ Read CPU time counters from /proc/stat.
    Values are cumulative CPU seconds.
    """
    clk_tck = os.sysconf(os.sysconf_names["SC_CLK_TCK"])

    with open("/proc/stat", "r") as f:
        for line in f:
            parts = line.split()
            if not parts or parts[0] != "cpu":
                break

            values = parts[1:6]
            if len(values) < 5:
                logger.error("Unexpected CPU data: %s", line.strip())
                break

            selected = {
                "user": values[0],
                "system": values[2],
                "idle": values[3],
                "iowait": values[4]
            }

            for mode in CPU_MODES:
                seconds = int(selected[mode]) / clk_tck

                metrics.append({
                    "metric": "node_cpu_seconds_total",
                    "mode": mode,
                    "value": seconds
                })


def read_memory(metrics):
    """ Read current counters from /proc/meminfo. """
    with open("/proc/meminfo", "r") as f:
        for line in f:
            parts = line.split()

            if len(parts) < 2:
                continue

            field = parts[0].rstrip(":")
            if field not in MEMORY_METRICS:
                continue

            value = int(parts[1])
            # /proc/meminfo values are kB.
            value_bytes = value * 1024

            metrics.append({
                "metric": MEMORY_METRICS[field],
                "value": value_bytes
            })


def read_filesystem_usage(metrics):
    """ Get FS usage from /proc/mounts. """
    with open("/proc/mounts", "r") as f:
        for line in f:
            parts = line.split()
            if len(parts) < 3:
                continue

            device = parts[0]
            mountpoint = parts[1]
            fstype = parts[2]

            if fstype not in ALLOWED_FSTYPES:
                continue

            try:
                stat = os.statvfs(mountpoint)
            except OSError:
                continue

            size_bytes = stat.f_blocks * stat.f_frsize
            avail_bytes = stat.f_bavail * stat.f_frsize

            metrics.append({
                "metric": "node_filesystem_size_bytes",
                "device": device,
                "mountpoint": mountpoint,
                "fstype": fstype,
                "value": size_bytes
            })

            metrics.append({
                "metric": "node_filesystem_avail_bytes",
                "device": device,
                "mountpoint": mountpoint,
                "fstype": fstype,
                "value": avail_bytes
            })


def read_disk_io(metrics):
    """ Get disk cumulative IO metrics. """
    with open("/proc/diskstats", "r") as f:
        for line in f:
            parts = line.split()
            if len(parts) < 14:
                continue

            device = parts[2]
            if device.startswith("sr") or device.startswith("loop"):
                continue

            sys_block = Path("/sys/block") / device
            if not sys_block.exists():
                continue

            if (sys_block / "partition").exists():
                continue

            metrics.append({
                "metric": "node_disk_reads_completed_total",
                "device": device,
                "value": int(parts[3])
            })

            metrics.append({
                "metric": "node_disk_read_bytes_total",
                "device": device,
                "value": int(parts[5]) * 512
            })

            metrics.append({
                "metric": "node_disk_writes_completed_total",
                "device": device,
                "value": int(parts[7])
            })

            metrics.append({
                "metric": "node_disk_written_bytes_total",
                "device": device,
                "value": int(parts[9]) * 512
            })

            metrics.append({
                "metric": "node_disk_read_time_seconds_total",
                "device": device,
                "value": int(parts[6]) / 1000.0
            })

            metrics.append({
                "metric": "node_disk_write_time_seconds_total",
                "device": device,
                "value": int(parts[10]) / 1000.0
            })

            metrics.append({
                "metric": "node_disk_io_time_seconds_total",
                "device": device,
                "value": int(parts[12]) / 1000.0
            })


def read_container_cpu_usage(cgroup_paths, metrics):
    """ Cumulative counter for container CPU usage. """
    for container, cgroup_path in cgroup_paths.items():
        try:
            stats = {}
            with (cgroup_path / "cpu.stat").open() as f:
                for line in f:
                    key, value = line.split()
                    if key == "usage_usec":
                        stats[key] = int(value)
                        break

            if stats:
                metrics.append({
                    "metric": "container_cpu_usage_seconds_total",
                    "container": container,
                    "value": stats["usage_usec"] / 1_000_000
                })

        except OSError:
            continue


def read_container_memory_usage(cgroup_paths, metrics):
    """ Returns non-cumulative counter for container memory usage. """
    for container, cgroup_path in cgroup_paths.items():
        try:
            value = int((cgroup_path / "memory.current").read_text().strip())
        except OSError:
            continue

        metrics.append({
            "metric": "container_memory_usage_bytes",
            "container": container,
            "value": value
        })


def read_container_fs_io(cgroup_paths, metrics):
    """ Returns cumulative metrics for container FS usage. """
    for container, cgroup_path in cgroup_paths.items():
        read_bytes = 0
        write_bytes = 0

        try:
            with (cgroup_path / "io.stat").open() as f:
                for line in f:
                    parts = line.split()

                    for field in parts[1:]:
                        key, value = field.split("=", 1)
                        if key == "rbytes":
                            read_bytes += int(value)
                        elif key == "wbytes":
                            write_bytes += int(value)

        except OSError:
            continue

        metrics.append({
            "metric": "container_fs_reads_bytes_total",
            "container": container,
            "value": read_bytes
        })

        metrics.append({
            "metric": "container_fs_writes_bytes_total",
            "container": container,
            "value": write_bytes
        })


def read_pgdata_usage(metrics):
    """ Compute size for PGDATA subfolders. """
    sizes = get_pgdata_bytes()

    if sizes is None:
        return

    log_size_bytes, wal_size_bytes, data_size_bytes = sizes

    metrics.append({
        "metric": "postgres_data_size_bytes",
        "value": data_size_bytes
    })

    metrics.append({
        "metric": "postgres_wal_size_bytes",
        "value": wal_size_bytes
    })

    metrics.append({
        "metric": "postgres_log_size_bytes",
        "value": log_size_bytes
    })


# ------ Util funcs -------------------------------------------------------------------------------
def get_docker_cgroup_path(container):
    """ Get cgroup path for docker container. """
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Pid}}", container],
        capture_output=True,
        text=True,
        check=True,
    )
    pid = result.stdout.strip()

    with open(f"/proc/{pid}/cgroup") as f:
        for line in f:
            if line.startswith("0::"):
                cgroup_path = line.strip().split("::", 1)[1]
                return Path("/sys/fs/cgroup") / cgroup_path.lstrip("/")

    raise RuntimeError(f"cgroup v2 path not found for {container}")


def get_podman_cgroup_path(container):
    """ Get cgroup path for podman container. """
    result = subprocess.run(
        ["podman", "inspect", container, "--format", "{{.State.CgroupPath}}"],
        capture_output=True,
        text=True,
        check=True,
    )
    cgroup_path = result.stdout.strip()
    if cgroup_path:
        return Path("/sys/fs/cgroup") / cgroup_path.lstrip("/")

    raise RuntimeError(f"cgroup v2 path not found for {container}")


def get_pgdata_bytes():
    """ Run du -s on the db container. """
    try:
        result = subprocess.run(
            [
                MANAGER,
                "exec",
                "emhealth-db",
                "sh",
                "-c",
                'du -s $PGDATA/log $PGDATA/pg_wal $PGDATA',
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        sizes = [int(line.split()[0]) * 1024 for line in result.stdout.splitlines()]

        if len(sizes) != 3:
            logger.error("du cmd failed, expected 3 values, got %d", len(sizes))
            return None

        return tuple(sizes)

    except Exception as e:
        logger.exception("Failed to get PGDATA sizes: %s", e)
        return None


def get_os_name():
    """ Parse Linux OS name. """
    with open("/etc/os-release") as f:
        for line in f:
            line = line.strip()
            if line.startswith("PRETTY_NAME"):
                return line.split("=")[-1].strip('"')
    return None


def acquire_lock():
    lock_path = "/tmp/emhealth-system-collector.lock"
    lock_file = open(lock_path, "w")

    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB,)
    except BlockingIOError:
        lock_file.close()
        raise RuntimeError("Another system collector is already running")

    return lock_file


# --------------------------------------------------------------------------------------------------
class SystemCollector:
    def __init__(self):
        self.cgroup_paths: dict[str, Path] = {}

    def collect_fast(self):
        metrics = []

        readers = (
            read_loadavg,
            read_cpu_seconds,
            read_memory,
            read_filesystem_usage,
            read_disk_io,
        )

        for reader in readers:
            try:
                reader(metrics)
            except Exception:
                logger.exception("Metric collection failed: %s", reader.__name__)

        try:
            # Discover missing cgroups and refresh paths after container recreation
            self._refresh_cgroup_paths()
        except Exception:
            logger.exception("Failed to refresh cgroup paths")
            return metrics

        cgroup_readers = (
            read_container_cpu_usage,
            read_container_memory_usage,
            read_container_fs_io,
        )

        for reader in cgroup_readers:
            try:
                reader(self.cgroup_paths, metrics)
            except Exception:
                logger.exception("Container metric collection failed: %s", reader.__name__)

        return metrics

    def collect_slow(self):
        metrics = []

        readers = (
            read_pgdata_usage,
        )

        for reader in readers:
            try:
                reader(metrics)
            except Exception:
                logger.exception("Metric collection failed: %s", reader.__name__)

        return metrics

    def _get_cgroup_path(self, container):
        path = self.cgroup_paths.get(container)

        if path is not None and path.exists():
            return path

        try:
            if MANAGER == "docker":
                path = get_docker_cgroup_path(container)
            else:
                path = get_podman_cgroup_path(container)

        except (subprocess.CalledProcessError, RuntimeError, OSError):
            logger.warning("Unable to find cgroup for %s", container)
            self.cgroup_paths.pop(container, None)
            return None

        self.cgroup_paths[container] = path
        return path

    def _refresh_cgroup_paths(self):
        for container in CONTAINERS:
            self._get_cgroup_path(container)


def run_collector(collector, db):
    next_fast = time.monotonic()
    next_slow = next_fast

    while True:
        now = time.monotonic()
        if now >= next_fast:
            collection_time = datetime.now(timezone.utc)

            try:
                metrics = collector.collect_fast()
                logger.debug(json.dumps(metrics, sort_keys=True, indent=2))
            except Exception:
                logger.exception("Fast collection failed")
            else:
                db.run_query("SELECT pganalyze.import_sysstats(%s, %s)",
                             values=(collection_time, Jsonb(metrics)))

            next_fast += FAST_INTERVAL

        if now >= next_slow:
            collection_time = datetime.now(timezone.utc)

            try:
                metrics = collector.collect_slow()
                logger.debug(json.dumps(metrics, sort_keys=True, indent=2))
            except Exception:
                logger.exception("Slow collection failed")
            else:
                db.run_query("SELECT pganalyze.import_sysstats(%s, %s)",
                             values=(collection_time, Jsonb(metrics)))

            next_slow += SLOW_INTERVAL

        next_run = min(next_fast, next_slow)
        sleep_for = next_run - time.monotonic()

        if sleep_for > 0:
            time.sleep(sleep_for)


def main():
    try:
        lock_file = acquire_lock()
    except Exception as e:
        logger.error("%s", e)
        return

    os_name = get_os_name()
    hostname = platform.node()
    kernel_name = platform.system()
    kernel_version = platform.release()
    cpu_count = os.cpu_count() or 1

    static_metrics = {
        "cpu_count": cpu_count,
        "hostname": hostname,
        "os_name": os_name,
        "kernel_name": kernel_name,
        "kernel_version": kernel_version
    }

    collector = SystemCollector()
    for attempt in range(1, DB_MAX_ATTEMPTS + 1):
        try:
            with DatabaseAnalyzer("tem", username="pganalyze", password="POSTGRES_PGANALYZE_PASSWORD") as db:
                db.run_query("SELECT pganalyze.import_sysinfo(%s)",
                             values=(Jsonb(static_metrics),))
                logger.debug(json.dumps(static_metrics, sort_keys=True, indent=2))
                run_collector(collector, db)

        except Exception:
            logger.exception("Collector/DB session failed (%d/%d)", attempt, DB_MAX_ATTEMPTS)
            if attempt < DB_MAX_ATTEMPTS:
                time.sleep(DB_RETRY_INTERVAL)

    logger.error("Collector stopped after %d failed DB attempts", DB_MAX_ATTEMPTS)


if __name__ == "__main__":
    main()
