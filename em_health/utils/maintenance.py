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
from datetime import datetime
from pathlib import Path

from em_health.utils.tools import logger, run_command

DOCKER_COMPOSE_FILE = "compose.yaml"
PG_CONTAINER = "timescaledb"
GRAFANA_CONTAINER = "grafana"
BACKUP_PATH = Path("backups")


def chdir_docker_dir() -> None:
    """Change working directory to em_health/docker."""
    package_root = Path(__file__).resolve().parents[2] / "docker"
    os.chdir(package_root)


def get_tsdb_version(dbname: str) -> str:
    """Retrieve TimescaleDB extension version from a running container."""
    result = run_command(
        f"docker exec {PG_CONTAINER} psql -d {dbname} -t -c "
        "\"SELECT extversion FROM pg_extension WHERE extname='timescaledb';\"",
        capture_output=True)
    return result.stdout.strip()


def get_tsdb_version_from_backup(fn: Path) -> str | None:
    """Extract TimescaleDB version from backup filename."""
    try:
        return fn.name.split("_")[2]
    except IndexError:
        logger.warning("Could not determine Timescale version from backup filename: %s", fn)
        return None


def erase_db(dbname: str, ts_version: str | None = None, do_init: bool = False) -> None:
    """Erase existing DB and optionally re-initialize it."""
    version_clause = f" VERSION '{ts_version}'" if ts_version else ""
    cmd = f"""
docker exec {PG_CONTAINER} bash -c "\
psql -d postgres -c \\"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{dbname}';\\" && \
psql -d postgres -c \\"DROP DATABASE IF EXISTS {dbname};\\" && \
psql -d postgres -c \\"CREATE DATABASE {dbname};\\" && \
psql -d tem -c \\"CREATE EXTENSION IF NOT EXISTS timescaledb{version_clause} CASCADE; CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit CASCADE;\\""
"""
    run_command(cmd)

    if do_init:
        run_command(
            f'docker exec {PG_CONTAINER} bash -c "psql -v ON_ERROR_STOP=1 -d {dbname} '
            '-f /docker-entrypoint-initdb.d/init_db.sql"'
        )


def backup(dbname: str = "tem") -> tuple[Path, Path]:
    """Backup TimescaleDB and Grafana."""
    chdir_docker_dir()
    timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")
    ts_version = get_tsdb_version(dbname)

    pg_backup = BACKUP_PATH / f"pg_{dbname}_{ts_version}_{timestamp}.dump"
    grafana_backup = BACKUP_PATH / f"grafana_{timestamp}.db"

    # TimescaleDB backup
    logger.info("Backing up TimescaleDB '%s' to %s", dbname, pg_backup.resolve())
    run_command(f"docker exec {PG_CONTAINER} pg_dump -Fc -d {dbname} -f /{pg_backup}")

    # Grafana backup
    logger.info("Backing up Grafana DB to %s", grafana_backup.resolve())
    run_command(f"docker stop {GRAFANA_CONTAINER}")
    run_command(f"docker cp {GRAFANA_CONTAINER}:/var/lib/grafana/grafana.db {grafana_backup}")
    run_command(f"docker start {GRAFANA_CONTAINER}")

    return pg_backup, grafana_backup


def list_backups() -> list[Path]:
    """Return a list of backup files."""
    chdir_docker_dir()
    return [f for f in BACKUP_PATH.iterdir() if f.suffix in (".db", ".dump")]


def restore(dbname: str, backup_file: Path) -> None:
    """Restore TimescaleDB or Grafana from a backup file."""
    chdir_docker_dir()

    if backup_file.suffix == ".db":
        # Grafana restore
        logger.info("Restoring Grafana DB from %s", backup_file)
        commands = [
            f"docker stop {GRAFANA_CONTAINER}",
            f"docker run --rm -v emhealth_grafana-storage:/var/lib/grafana "
            f"-v ./backups:/backups busybox sh -c '"
            f"cp {backup_file} /var/lib/grafana/grafana.db && "
            "chown 472:root /var/lib/grafana/grafana.db'",
            f"docker start {GRAFANA_CONTAINER}",
        ]
        for cmd in commands:
            run_command(cmd)

    else:
        # TimescaleDB restore
        logger.info("Restoring TimescaleDB '%s' from %s", dbname, backup_file)
        ts_version = get_tsdb_version_from_backup(backup_file)
        erase_db(dbname, ts_version, do_init=False)

        restore_cmd = f"""
docker exec {PG_CONTAINER} bash -c "\
psql -d {dbname} -c \\"SELECT timescaledb_pre_restore();\\" && \
pg_restore -Fc -d {dbname} /{backup_file} && \
psql -d {dbname} -c \\"SELECT timescaledb_post_restore(); ANALYZE;\\""
"""
        run_command(restore_cmd)

    logger.info("Restore completed")


def update() -> None:
    """Migrate DB schema, backup, update containers restore safely."""
    from em_health.db_manager import main as db_manager
    db_manager("tem", "migrate")

    pg_backup, grafana_backup = backup("tem")

    chdir_docker_dir()
    for cmd in [
        f"docker compose -f {DOCKER_COMPOSE_FILE} down",
        f"docker compose -f {DOCKER_COMPOSE_FILE} pull",
        f"docker compose -f {DOCKER_COMPOSE_FILE} up -d",
        "docker image prune -f",
    ]:
        run_command(cmd)

    restore("tem", pg_backup)
    restore("tem", grafana_backup)

    # Update extensions
    run_command(
        f'docker exec {PG_CONTAINER} psql -d tem -c "ALTER EXTENSION timescaledb UPDATE; '
        'ALTER EXTENSION timescaledb_toolkit UPDATE;"'
        'ALTER EXTENSION tds_fdw UPDATE;"'
    )

    logger.info("Finished updating")


def main(dbname: str, action: str) -> None:
    """Run update/backup/restore interactively."""
    if action == "update":
        update()

    elif action == "backup":
        backup(dbname)

    elif action == "restore":
        confirm = input("Restoring will DELETE existing database.\nType YES to continue: ")
        if confirm != "YES":
            logger.warning("Restore aborted by user.")
            return

        backups = list_backups()
        if not backups:
            logger.error("No backups found.")
            return

        print("Available backups:")
        for i, f in enumerate(backups, 1):
            print(f"{i}. {f}")

        choice = input(f"Select a backup to restore (1-{len(backups)}): ").strip()
        if not choice.isdigit() or not (1 <= int(choice) <= len(backups)):
            logger.error("Invalid backup choice.")
            return

        restore(dbname, backups[int(choice) - 1])
