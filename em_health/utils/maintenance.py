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
from datetime import datetime
from pathlib import Path
from packaging.version import Version

from em_health.utils.tools import logger, run_command

MANAGER = os.getenv("MANAGER_TYPE")
DOCKER_COMPOSE_FILE = "compose.yaml"
PG_CONTAINER = "timescaledb"
GRAFANA_CONTAINER = "grafana"
BACKUP_HOST_PATH = Path(os.getenv("BACKUP_DIR"))


def chdir_docker_dir() -> None:
    """Change working directory to em_health/docker."""
    package_root = Path(__file__).resolve().parents[2] / "docker"
    os.chdir(package_root)


def get_ts_version(dbname: str) -> str:
    """Retrieve TimescaleDB extension version from a running container."""
    result = run_command(
        f"{MANAGER} exec {PG_CONTAINER} psql -d {dbname} -t -c "
        "\"SELECT extversion FROM pg_extension WHERE extname='timescaledb';\"",
        capture_output=True)
    return result.stdout.strip()


def get_pg_version() -> str:
    """Retrieve Postgres version from a running container."""
    result = run_command(
        f"{MANAGER} exec {PG_CONTAINER} psql -d tem -t -c "
        "\"SELECT current_setting('server_version_num');\"",
        capture_output=True)
    return result.stdout.strip()

def check_versions(dbname: str, fn: Path):
    """Compare backup file with server versions."""
    pg_version = fn.name.split("_")[2]
    ts_version = fn.name.split("_")[3]

    pg_version_server = get_pg_version()
    ts_version_server = get_ts_version(dbname)

    if pg_version != pg_version_server:
        logger.warning(f"Postgres version mismatch: server {pg_version_server}, backup {pg_version}")

    if ts_version != ts_version_server:
        logger.warning(f"Timescale version mismatch: server {ts_version_server} (db {dbname}), backup {ts_version}")


def erase_db(dbname: str, ts_version: str | None = None, do_init: bool = False) -> None:
    """Erase existing DB and optionally re-initialize it."""
    version_clause = f" VERSION '{ts_version}'" if ts_version else ""
    cmd = f"""
{MANAGER} exec {PG_CONTAINER} bash -c "\
psql -d postgres -c \\"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{dbname}';\\" && \
psql -d postgres -c \\"DROP DATABASE IF EXISTS {dbname};\\" && \
psql -d postgres -c \\"CREATE DATABASE {dbname};\\" && \
psql -d {dbname} -c \\"CREATE EXTENSION IF NOT EXISTS timescaledb{version_clause} CASCADE; CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit CASCADE;\\""
"""
    run_command(cmd)

    if do_init:
        run_command(f'{MANAGER} exec {PG_CONTAINER} /docker-entrypoint-initdb.d/init_db.sh')


def backup() -> list[Path]:
    """Backup TimescaleDB and Grafana."""
    outputs = []
    timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")
    pg_version = get_pg_version()

    # TimescaleDB backup
    for dbname in ["tem", "sem"]:
        ts_version = get_ts_version(dbname)
        pg_backup = Path("/backups") / f"pg_{dbname}_{pg_version}_{ts_version}_{timestamp}.dump"
        pg_host_backup = BACKUP_HOST_PATH / f"pg_{dbname}_{pg_version}_{ts_version}_{timestamp}.dump"
        logger.info("Backing up TimescaleDB '%s' to %s", dbname, pg_host_backup.resolve())
        run_command(f"{MANAGER} exec {PG_CONTAINER} pg_dump -Fc -d {dbname} -f {pg_backup}")
        outputs.append(pg_backup)

    # Grafana backup
    grafana_backup = BACKUP_HOST_PATH / f"grafana_{timestamp}.db"
    logger.info("Backing up Grafana DB to %s", grafana_backup.resolve())
    run_command(f"{MANAGER} stop {GRAFANA_CONTAINER}")
    run_command(f"{MANAGER} cp {GRAFANA_CONTAINER}:/var/lib/grafana/grafana.db {grafana_backup}")
    run_command(f"{MANAGER} start {GRAFANA_CONTAINER}")
    outputs.append(grafana_backup)

    return outputs


def list_backups() -> list[Path]:
    """Return a list of backup files."""
    return [f for f in BACKUP_HOST_PATH.iterdir() if f.suffix in (".db", ".dump")]


def restore(backup_file: Path, update: bool = False) -> None:
    """Restore TimescaleDB or Grafana from a backup file."""
    if backup_file.suffix == ".db":
        # Grafana restore
        logger.info("Restoring Grafana DB from %s", backup_file)
        commands = [
            f"{MANAGER} stop {GRAFANA_CONTAINER}",
            f"{MANAGER} run --rm -v emhealth_grafana-storage:/var/lib/grafana "
            f"-v {BACKUP_HOST_PATH}:/backups busybox sh -c '"
            f"cp /backups/{backup_file.name} /var/lib/grafana/grafana.db && "
            "chown 472:root /var/lib/grafana/grafana.db'",
            f"{MANAGER} start {GRAFANA_CONTAINER}",
        ]
        if update:
            commands.append(f"{MANAGER} exec {GRAFANA_CONTAINER} grafana cli plugins update-all")
        for cmd in commands:
            run_command(cmd)

    else:
        # TimescaleDB restore
        dbname = backup_file.name.split("_")[1]
        check_versions(dbname, backup_file)
        ts_version = backup_file.name.split("_")[3]
        logger.info("Restoring TimescaleDB %s '%s' from %s", ts_version, dbname, backup_file)
        erase_db(dbname, ts_version, do_init=False)

        restore_cmd = f"""
{MANAGER} exec {PG_CONTAINER} bash -c "\
psql -d {dbname} -c \\"SELECT timescaledb_pre_restore();\\" && \
pg_restore -Fc -d {dbname} /backups/{backup_file.name} && \
psql -d {dbname} -c \\"SELECT timescaledb_post_restore(); ANALYZE;\\""
"""
        run_command(restore_cmd)

    logger.info("Restore completed")


def update() -> None:
    """Update everything."""
    from em_health import __version__
    if (Version(__version__) >= Version("0.1a5")) and get_pg_version().startswith("17"):
        raise ValueError("EMHealth 0.1a5+ does not support PostgreSQL 17. Check documentation at https://em-health.readthedocs.io/latest/maintenance.html#updating-postgresql-from-v17-to-v18")

    # migrate db schema
    from em_health.db_manager import main as db_manager
    db_manager("tem", "migrate")
    db_manager("sem", "migrate")

    # backup db
    pg_backup_tem, pg_backup_sem, grafana_backup = backup()

    # update containers
    chdir_docker_dir()

    if MANAGER == "podman":
        mgr_cmd = "podman-compose"
    else:
        mgr_cmd = f"docker compose"

    for cmd in [
        f"{mgr_cmd} -f {DOCKER_COMPOSE_FILE} down",
        f"{mgr_cmd} -f {DOCKER_COMPOSE_FILE} pull",
        f"{mgr_cmd} -f {DOCKER_COMPOSE_FILE} up -d",
        f"{MANAGER} image prune -f",
    ]:
        run_command(cmd)

    # restore backups
    restore(pg_backup_tem)
    restore(pg_backup_sem)
    # update extensions if possible
    for dbname in ["tem", "sem"]:
        run_command(
            f'{MANAGER} exec {PG_CONTAINER} psql -X -d {dbname} -c "ALTER EXTENSION timescaledb UPDATE;'
            'ALTER EXTENSION timescaledb_toolkit UPDATE;'
            'ALTER EXTENSION tds_fdw UPDATE;"'
        )

    restore(grafana_backup, update=True)

    logger.info("Finished updating")


def main(action: str) -> None:
    """Run update/backup/restore interactively."""
    if action == "update":
        update()

    elif action == "backup":
        backup()

    elif action == "restore":
        confirm = input("Restoring will DELETE existing database.\nType YES to continue: ")
        if confirm != "YES":
            print("Restore aborted by user.")
            return

        backups = list_backups()
        if not backups:
            print("No backups found.")
            return

        print("Available backups:")
        for i, f in enumerate(backups, 1):
            print(f"{i}. {f}")

        choice = input(f"Select a backup to restore (1-{len(backups)}): ").strip()
        if not choice.isdigit() or not (1 <= int(choice) <= len(backups)):
            print("Invalid backup choice.")
            return

        restore(backups[int(choice) - 1])
